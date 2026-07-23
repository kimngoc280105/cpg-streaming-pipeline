import json
from pathlib import Path


ROOT = Path(__file__).parents[1]
CONNECT = ROOT / "infra/connect"


def load_connector_config():
    return json.loads((CONNECT / "neo4j-sink-config.json").read_text(encoding="utf-8"))


def test_sink_consumes_only_graph_topics_and_uses_secret_provider():
    config = load_connector_config()

    assert config["connector.class"] == "org.neo4j.connectors.kafka.sink.Neo4jConnector"
    assert config["tasks.max"] == "1"
    assert set(config["topics"].split(",")) == {"cpg.nodes.v1", "cpg.edges.v1"}
    assert "metadata" not in config["topics"]
    assert config["neo4j.authentication.basic.password"] == "${env:LAB04_NEO4J_PASSWORD}"


def test_node_and_edge_cypher_are_idempotent_and_support_deletes():
    config = load_connector_config()
    node_cypher = config["neo4j.cypher.topic.cpg.nodes.v1"]
    edge_cypher = config["neo4j.cypher.topic.cpg.edges.v1"]

    assert "MERGE (node:CPGNode {id: event.node.id})" in node_cypher
    assert "DETACH DELETE obsolete" in node_cypher

    assert "MERGE (source:CPGNode {id: event.edge.source_id})" in edge_cypher
    assert "MERGE (target:CPGNode {id: event.edge.target_id})" in edge_cypher
    assert "MERGE (source)-[edge:CPG_EDGE {id: event.edge.id}]->(target)" in edge_cypher
    assert "DELETE obsolete" in edge_cypher


def test_dlq_constraints_and_registration_contract():
    config = load_connector_config()
    init_cypher = (ROOT / "infra/neo4j/init.cypher").read_text(encoding="utf-8")
    register = (CONNECT / "register.sh").read_text(encoding="utf-8")
    wait = (CONNECT / "register-wait.sh").read_text(encoding="utf-8")
    dockerfile = (CONNECT / "Dockerfile").read_text(encoding="utf-8")

    assert config["errors.tolerance"] == "all"
    assert config["errors.deadletterqueue.topic.name"] == "cpg.neo4j-dlq.v1"
    assert "REQUIRE node.id IS UNIQUE" in init_cypher
    assert "CPG_EDGE" in init_cypher and "edge.id" in init_cypher

    assert 'name="cpg-neo4j-sink"' in register
    assert "-X PUT" in register and "-X POST" in register
    assert '"connector":{"state":"RUNNING"' in wait
    assert '"tasks":\\[{"id":0,"state":"RUNNING"' in wait

    assert "confluentinc/cp-kafka-connect:7.8.0" in dockerfile
    assert "NEO4J_CONNECTOR_VERSION=5.5.0" in dockerfile
