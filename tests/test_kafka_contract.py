from datetime import datetime
from pathlib import Path

from cpg_parser.constants import (
    SCHEMA_VERSION,
    TOPIC_EDGES,
    TOPIC_ERRORS,
    TOPIC_METADATA,
    TOPIC_NEO4J_DLQ,
    TOPIC_NODES,
)
from cpg_parser.manifest import Manifest
from cpg_parser.publisher import MemoryPublisher
from cpg_parser.service import ParserService


ROOT = Path(__file__).parents[1]


def test_topic_initialization_matches_task3_contract():
    script = (ROOT / "infra/kafka/create-topics.sh").read_text(encoding="utf-8")

    expected = {
        TOPIC_NODES: "compact",
        TOPIC_EDGES: "compact",
        TOPIC_METADATA: "compact",
        TOPIC_ERRORS: "delete",
        TOPIC_NEO4J_DLQ: "delete",
    }
    for topic, cleanup_policy in expected.items():
        assert f"create_topic {topic} {cleanup_policy}" in script

    assert "--partitions 1" in script
    assert "--replication-factor 1" in script
    assert "retention.ms=604800000" in script


def test_all_required_topics_have_keys_version_and_utc_event_time(tmp_path):
    records = []

    valid_publisher = MemoryPublisher()
    with Manifest(tmp_path / "valid.sqlite") as manifest:
        service = ParserService(
            ROOT / "tests/fixtures/sample_repo",
            "fixture/sample",
            valid_publisher,
            manifest,
        )
        service.process("app.py", force=True)
    records.extend(valid_publisher.records)

    error_publisher = MemoryPublisher()
    with Manifest(tmp_path / "error.sqlite") as manifest:
        service = ParserService(
            ROOT / "tests/fixtures/invalid_repo",
            "fixture/invalid",
            error_publisher,
            manifest,
        )
        service.process("broken.py", force=True)
    records.extend(error_publisher.records)

    required_topics = {TOPIC_NODES, TOPIC_EDGES, TOPIC_METADATA, TOPIC_ERRORS}
    assert required_topics <= {topic for topic, _, _ in records}

    for topic, key, value in records:
        assert key
        assert value["schema_version"] == SCHEMA_VERSION
        assert value["event_time"].endswith("Z")
        datetime.fromisoformat(value["event_time"].replace("Z", "+00:00"))

        if topic == TOPIC_NODES:
            assert key == value["node"]["id"]
        elif topic == TOPIC_EDGES:
            assert key == value["edge"]["id"]
        elif topic == TOPIC_METADATA:
            assert key == value["file_id"] == value["metadata"]["_id"]
        elif topic == TOPIC_ERRORS:
            assert key == value["event_id"] == value["error"]["id"]
