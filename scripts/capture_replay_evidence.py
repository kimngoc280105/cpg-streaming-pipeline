from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from cpg_parser.ids import file_id as sha256_file_id


def validate_evidence(evidence: Dict[str, Any]) -> Dict[str, bool]:
    stages = evidence.get("stages", {})
    baseline = stages.get("baseline", {})
    modified = stages.get("modified", {})
    forced_unchanged = stages.get("forced_unchanged", {})
    restart_replay = stages.get("restart_replay", {})
    repo = evidence.get("repository", {})

    b_nodes = baseline.get("neo4j", {}).get("nodes", 0)
    b_edges = baseline.get("neo4j", {}).get("edges", 0)
    b_file_nodes = baseline.get("neo4j", {}).get("file_nodes", 0)
    b_file_edges = baseline.get("neo4j", {}).get("file_edges", 0)

    m_nodes = modified.get("neo4j", {}).get("nodes", 0)
    m_edges = modified.get("neo4j", {}).get("edges", 0)
    m_file_nodes = modified.get("neo4j", {}).get("file_nodes", 0)
    m_file_edges = modified.get("neo4j", {}).get("file_edges", 0)
    m_del_edges = modified.get("parser", {}).get("deleted_edges", 0)

    u_nodes = forced_unchanged.get("neo4j", {}).get("nodes", 0)
    u_edges = forced_unchanged.get("neo4j", {}).get("edges", 0)
    u_docs = forced_unchanged.get("mongo", {}).get("documents", 0)

    r_nodes = restart_replay.get("neo4j", {}).get("nodes", 0)
    r_offset = restart_replay.get("mongo", {}).get("document", {}).get("kafka_offset", 0)
    m_offset = modified.get("mongo", {}).get("document", {}).get("kafka_offset", 0)

    return {
        "baseline_processed_files_count": repo.get("processed_python_files", 0) == 61,
        "modified_file_nodes_increased": m_file_nodes > b_file_nodes,
        "modified_file_edges_increased": m_file_edges > b_file_edges,
        "modified_neo4j_total_nodes_updated": m_nodes == b_nodes + (m_file_nodes - b_file_nodes),
        "modified_neo4j_total_edges_updated": m_edges == b_edges + (m_file_edges - b_file_edges),
        "modified_neo4j_unique_nodes_match_total": modified.get("neo4j", {}).get("unique_nodes") == m_nodes,
        "modified_neo4j_unique_edges_match_total": modified.get("neo4j", {}).get("unique_edges") == m_edges,
        "modified_mongo_hash_updated": modified.get("mongo", {}).get("document", {}).get("content_hash") == modified.get("source_hash"),
        "modified_mongo_doc_count_unchanged": modified.get("mongo", {}).get("documents") == baseline.get("mongo", {}).get("documents"),
        "modified_mongo_distinct_files_unchanged": modified.get("mongo", {}).get("distinct_files") == baseline.get("mongo", {}).get("documents"),
        "unchanged_replay_neo4j_nodes_equal": u_nodes == m_nodes,
        "unchanged_replay_neo4j_edges_equal": u_edges == m_edges,
        "unchanged_replay_mongo_docs_equal": u_docs == modified.get("mongo", {}).get("documents"),
        "restart_replay_neo4j_nodes_equal": r_nodes == m_nodes,
        "restart_replay_mongo_offset_advanced": r_offset >= m_offset,
        "neo4j_dlq_empty": evidence.get("neo4j_dlq_end_offset", -1) == 0,
    }


def main():
    print("Capture replay evidence helper script.")


if __name__ == "__main__":
    main()
