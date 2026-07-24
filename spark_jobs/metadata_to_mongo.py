from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)


metadata_schema = StructType(
    [
        StructField("_id", StringType(), False),
        StructField("repo_id", StringType(), False),
        StructField("repo_url", StringType(), True),
        StructField("path", StringType(), False),
        StructField("content_hash", StringType(), False),
        StructField("size_bytes", IntegerType(), False),
        StructField("line_count", IntegerType(), False),
        StructField("commit_sha", StringType(), True),
        StructField("parse_status", StringType(), False),
        StructField("node_counts", MapType(StringType(), IntegerType()), True),
        StructField("edge_counts", MapType(StringType(), IntegerType()), True),
        StructField("warnings", ArrayType(StringType()), True),
        StructField("error_message", StringType(), True),
        StructField("processed_at", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("schema_version", StringType(), False),
    ]
)

event_schema = StructType(
    [
        StructField("schema_version", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("repo_id", StringType(), False),
        StructField("file_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("content_hash", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("op", StringType(), False),
        StructField("metadata", metadata_schema, False),
    ]
)


class Lab04ProgressListener(StreamingQueryListener):
    """Persist query lifecycle/progress evidence on the checkpoint volume."""

    def __init__(self, path: str) -> None:
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _append(self, payload: dict) -> None:
        record = {
            "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            **payload,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
            handle.flush()

    def onQueryStarted(self, event) -> None:  # noqa: N802
        self._append(
            {
                "type": "query_started",
                "query_id": str(event.id),
                "run_id": str(event.runId),
                "name": event.name,
            }
        )

    def onQueryProgress(self, event) -> None:  # noqa: N802
        self._append({"type": "query_progress", "progress": json.loads(event.progress.json)})

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        self._append(
            {
                "type": "query_terminated",
                "query_id": str(event.id),
                "run_id": str(event.runId),
                "exception": event.exception,
            }
        )


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    mongo_uri = os.getenv("MONGODB_URI", "mongodb://mongo:27017")
    database = os.getenv("MONGODB_DATABASE", "lab04")
    collection = os.getenv("MONGODB_COLLECTION", "source_metadata")
    checkpoint = os.getenv("CHECKPOINT_DIR", "/opt/checkpoints/source-metadata-v1")
    progress_log = os.getenv(
        "SPARK_PROGRESS_LOG", "/opt/checkpoints/source-metadata-progress.jsonl"
    )

    spark = (
        SparkSession.builder.appName("lab04-source-metadata-to-mongo")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(Lab04ProgressListener(progress_log))

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", "cpg.source-metadata.v1")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )

    parsed = kafka.select(
        from_json(col("value").cast("string"), event_schema).alias("event"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").cast("string").alias("kafka_timestamp"),
    )
    documents = parsed.select(
        "event.metadata.*", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp"
    ).where(col("_id").isNotNull())

    query = (
        documents.writeStream.format("mongodb")
        .queryName("lab04-source-metadata-to-mongo")
        .option("checkpointLocation", checkpoint)
        .option("spark.mongodb.connection.uri", mongo_uri)
        .option("spark.mongodb.database", database)
        .option("spark.mongodb.collection", collection)
        .option("operationType", "replace")
        .option("idFieldList", "_id")
        .option("upsertDocument", "true")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
