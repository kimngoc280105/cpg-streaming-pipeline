# Lab 04 - Incremental CPG Streaming Pipeline

Welcome to the Lab 04 report on **Incremental Code Property Graph (CPG) Construction and Streaming Ingestion**.

## Overview Architecture

```text
Python repository -> Parser Service -> Kafka nodes/edges -> Kafka Connect -> Neo4j
                                  \-> Kafka metadata -> Spark -> MongoDB
                                  \-> Kafka parser errors
```

This project implements:
1. **Repository Discovery & Shallow Clone**: Discovery of Python source files excluding tests, setup, and auto-generated files.
2. **Incremental CPG Parser Service**: Extraction of AST nodes, CFG edges, DFG edges, and CALL edges with deterministic SHA-256 stable identifiers.
3. **Kafka Topic Design**: Topic layout with compacted topics for topology/metadata and 7-day retention for errors.
4. **Neo4j Graph Topology Ingestion**: Direct Kafka Connect Sink ingestion using Cypher `MERGE` statements without an intermediate Spark layer.
5. **MongoDB Source Metadata Ingestion**: Spark Structured Streaming ingestion using MongoDB Spark Connector with offset checkpointing.
6. **Idempotent Replay Verification**: Demonstration of file modification reprocessing with strict count consistency and offset skipping.

## Table of Contents

- [Architecture Diagram](architecture.ipynb)
- [Task 1: Repository Cloning and File Discovery](task1_repository.ipynb)
- [Task 2: Incremental CPG Parser Service](task2_parser.ipynb)
- [Task 3: Kafka Topic Design](task3_kafka.ipynb)
- [Task 4: Graph Topology Ingestion into Neo4j](task4_neo4j.ipynb)
- [Task 5: Source Metadata Ingestion into MongoDB](task5_mongodb.ipynb)
- [Task 6: Idempotent Replay Verification](task6_replay.ipynb)
