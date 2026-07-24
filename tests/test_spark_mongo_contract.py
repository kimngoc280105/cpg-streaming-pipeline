import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

from spark_jobs.metadata_to_mongo import event_schema, metadata_schema


def test_spark_mongo_schema_contract():
    assert "schema_version" in event_schema.fieldNames()
    assert "event_time" in event_schema.fieldNames()
    assert "metadata" in event_schema.fieldNames()

    assert "_id" in metadata_schema.fieldNames()
    assert "path" in metadata_schema.fieldNames()
    assert "content_hash" in metadata_schema.fieldNames()
    assert "parse_status" in metadata_schema.fieldNames()


def test_spark_job_persists_progress_evidence_on_checkpoint_volume():
    job = (ROOT / "spark_jobs/metadata_to_mongo.py").read_text(encoding="utf-8")
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "Lab04ProgressListener" in job
    assert "spark.streams.addListener" in job
    assert "source-metadata-progress.jsonl" in job
    assert "SPARK_PROGRESS_LOG: /opt/checkpoints/source-metadata-progress.jsonl" in compose
