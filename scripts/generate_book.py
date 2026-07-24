"""Generate notebooks and execute every code cell against one evidence run."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import nbformat as nbf
from nbclient import NotebookClient

from cpg_parser.ids import file_id


ROOT = Path(__file__).parents[1]
BOOK = ROOT / "book"
REPO = ROOT / "source-repo"
REPO_ID = "huggingface/optimum"
REPO_URL = "https://github.com/huggingface/optimum"
REPLAY_FILE = "optimum/version.py"
REPLAY_FILE_ID = file_id(REPO_ID, REPLAY_FILE)
GENERATED: dict[str, object] = {}


def code_cell(source: str):
    return nbf.v4.new_code_cell(source)


def evidence_sha256() -> str:
    path = ROOT / "evidence" / "runtime" / "verification.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def load_evidence() -> dict:
    path = ROOT / "evidence" / "runtime" / "verification.json"
    evidence = json.loads(path.read_text(encoding="utf-8"))
    if evidence.get("schema_version") != "3.0":
        raise RuntimeError(
            "Replay evidence is stale. Run: python scripts/capture_replay_evidence.py"
        )
    assertions = evidence.get("assertions", {})
    failed = [name for name, passed in assertions.items() if not passed]
    if not assertions or failed:
        detail = ", ".join(failed) if failed else "assertions missing"
        raise RuntimeError(f"Replay evidence is not complete: {detail}")
    return evidence


def execute_notebooks(names: list[str]) -> None:
    executed = []
    digest = evidence_sha256()
    executed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    for name in names:
        nb = GENERATED[name]
        client = NotebookClient(
            nb,
            timeout=600,
            kernel_name="python3",
            allow_errors=False,
            resources={"metadata": {"path": str(BOOK)}},
        )
        client.execute()
        nb.metadata["lab04_execution"] = {
            "engine": "nbclient",
            "executed_at": executed_at,
            "evidence_sha256": digest,
            "working_directory": "book",
        }
        executed.append((BOOK / name, nb))
    for path, nb in executed:
        nbf.write(nb, path)


def notebook(title: str, intro: str, cells: list, reflection: str):
    nb = nbf.v4.new_notebook()
    nb.metadata.kernelspec = {"display_name": "Python 3", "language": "python", "name": "python3"}
    nb.metadata.language_info = {"name": "python", "version": "3.11"}
    nb.cells = [
        nbf.v4.new_markdown_cell(f"# {title}\n\n{intro}"),
        *cells,
        nbf.v4.new_markdown_cell(f"## Reflection\n\n{reflection}"),
    ]
    return nb


def image_cell(filename: str, caption: str):
    path = BOOK / "figures" / filename
    if not path.exists():
        return nbf.v4.new_markdown_cell(
            "**UI capture disclosure:** the executable query output above is current and "
            f"verified. `{path.relative_to(ROOT)}` is intentionally absent because the "
            "The required database UI capture is missing; no screenshot was fabricated. "
            "Capture the corresponding local UI before the final Moodle submission."
        )
    return nbf.v4.new_markdown_cell(f"![{caption}](figures/{filename})\n\n*{caption}*")


def write(name: str, nb) -> None:
    BOOK.mkdir(parents=True, exist_ok=True)
    GENERATED[name] = nb


def build_architecture() -> None:
    diagram = """```mermaid
flowchart LR
  R[Python repository] --> P[Parser Service]
  P --> N[cpg.nodes.v1]
  P --> E[cpg.edges.v1]
  P --> M[cpg.source-metadata.v1]
  P --> X[cpg.parser-errors.v1]
  N --> C[Kafka Connect]
  E --> C
  C --> G[Neo4j]
  C --> Q[cpg.neo4j-dlq.v1]
  M --> S[Spark Structured Streaming]
  S --> D[MongoDB]
  S --> K[(Checkpoint)]
  X --> L[Parser error evidence]
```

Graph topology goes directly from Kafka Connect to Neo4j. Spark is used only
for source metadata; parser failures and connector failures have separate paths.

## Approach and rationale

**Approach:** The pipeline separates graph topology, source metadata, parser
errors, and connector failures at the Kafka boundary. Kafka Connect owns the
node/edge branch, while one Spark Structured Streaming query owns only the
metadata branch and persists its checkpoint on a Docker volume.

**Why this approach:** The split follows the assignment's required sink paths
and gives each consumer the smallest possible contract. Sending topology
directly through Kafka Connect avoids an unnecessary Spark transformation and
keeps Neo4j ingestion independent from metadata analytics. Separate error paths
prevent a malformed source file or sink record from stopping valid traffic.

**Alternatives and trade-offs:** A single topic or a Spark job for both sinks
would reduce the number of components but couple unrelated schemas, recovery
semantics, and failure handling. One partition and one broker make ordering and
the classroom demonstration reproducible, at the cost of throughput and
production-grade availability."""
    services_source = """import subprocess
from pathlib import Path

root = Path('..').resolve()
result = subprocess.run(
    ['docker', 'compose', 'config', '--services'],
    cwd=root, capture_output=True, text=True, check=True,
)
print(result.stdout.rstrip())
required = {'broker', 'connect', 'neo4j', 'mongo', 'spark-metadata'}
assert required <= set(result.stdout.splitlines())
print('PASS: all required architecture services are declared')"""
    write(
        "architecture.ipynb",
        notebook(
            "Architecture",
            diagram,
            [code_cell(services_source)],
            """**Worked:** Kafka Connect and Spark operate on separate branches, so graph topology never passes through Spark.

**Failed:** Connector creation was asynchronous and an immediate status request could observe a temporary 404.

**Resolution:** `register-wait.sh` now retries until both the connector and its task are `RUNNING`. Single-node Kafka and replication factor one remain documented educational limits.""",
        ),
    )

def build_task1() -> None:
    evidence = load_evidence()
    repository = evidence["repository"]
    source = """import json
import subprocess
import sys
from pathlib import Path

root = Path('..').resolve()
sys.path.insert(0, str(root))
from cpg_parser.discovery import discover_repo

evidence = json.loads((root / 'evidence/runtime/verification.json').read_text(encoding='utf-8'))
repo = root / 'source-repo'
if not repo.is_dir():
    repo = root.parent / 'source-repo'
current = discover_repo(repo).as_dict()
is_shallow = subprocess.run(
    ['git', '-C', str(repo), 'rev-parse', '--is-shallow-repository'],
    capture_output=True, text=True, check=True,
).stdout.strip() == 'true'
summary = {
    'locked_baseline': evidence['repository'],
    'current_replay_worktree': {
        key: current[key] for key in (
            'repo_url', 'commit_sha', 'raw_python_files', 'processed_python_files',
            'excluded_python_files', 'total_lines', 'parseable_files',
            'parse_success_rate', 'has_branch', 'has_loop', 'has_call'
        )
    },
    'is_shallow_repository': is_shallow,
    'excluded_files': current['excluded_files'],
}
print(json.dumps(summary, indent=2, ensure_ascii=False))
assert current['commit_sha'] == evidence['repository']['commit_sha']
assert current['processed_python_files'] == evidence['repository']['processed_python_files']
assert is_shallow
print('PASS: shallow clone, locked commit, and discovery counts verified')"""
    write(
        "task1_repository.ipynb",
        notebook(
            "Task 1 - Repository cloning and file discovery",
            f"""The assigned repository is [{REPO_ID}]({REPO_URL}). The ignored
source tree is a shallow clone locked to `{repository['commit_sha']}`.
Baseline and post-replay line counts are reported separately.

```mermaid
flowchart LR
  U[Assigned public repository] --> C[Shallow clone]
  C --> L[Checkout locked commit]
  L --> R[Count every .py file]
  R --> F[Apply documented exclusions]
  F --> P[Parseability and feature scan]
```

## Approach and rationale

**Approach:** The bootstrap script performs a depth-one clone, pins the selected
Optimum commit, reports both raw and filtered Python counts, and applies one
explicit exclusion policy before parsing. The report preserves the locked
baseline count separately from the seven-line replay modification.

**Why this approach:** A moving default branch would make counts and CPG
evidence impossible to reproduce. Shallow cloning satisfies the download-size
requirement, while reporting raw and filtered counts makes optional exclusions
auditable rather than silently changing the denominator.

**Alternatives and trade-offs:** Vendoring the third-party repository would make
the submission self-contained but duplicate unrelated source and history.
Processing tests and generated files could increase coverage, but it would add
noise and cost without improving the demonstration; excluded paths are therefore
listed so the scope remains transparent.""",
            [code_cell(source)],
            f"""**Worked:** The shallow clone reports {repository['raw_python_files']} raw Python files and {repository['processed_python_files']} processed files with full parseability.

**Failed:** A plain shallow clone of a moving default branch could change the file counts, and the replay edit made the current line count differ from the baseline.

**Resolution:** The bootstrap script fetches and checks out the recorded commit, while the report records baseline and modified line counts separately.""",
        ),
    )

def build_task2() -> None:
    aggregate_source = """import json
import sys
from collections import Counter
from pathlib import Path

root = Path('..').resolve()
sys.path.insert(0, str(root))
from cpg_parser.analyzer import CPGAnalyzer
from cpg_parser.discovery import discover_repo
from cpg_parser.ids import file_id

repo = root / 'source-repo'
if not repo.is_dir():
    repo = root.parent / 'source-repo'
report = discover_repo(repo)
node_counts, edge_counts = Counter(), Counter()
node_ids, edge_ids = set(), set()
warnings = 0
for relative in report.files:
    result = CPGAnalyzer(
        (repo / relative).read_text(encoding='utf-8', errors='replace'),
        file_id('huggingface/optimum', relative), relative,
    ).analyze()
    node_counts.update(result.node_counts())
    edge_counts.update(result.edge_counts())
    node_ids.update(node.id for node in result.nodes)
    edge_ids.update(edge.id for edge in result.edges)
    warnings += len(result.warnings)
summary = {
    'repository': 'huggingface/optimum',
    'files': len(report.files),
    'nodes': sum(node_counts.values()),
    'edges': sum(edge_counts.values()),
    'node_counts': dict(sorted(node_counts.items())),
    'edge_counts': dict(sorted(edge_counts.items())),
    'warnings': warnings,
    'all_node_ids_unique': len(node_ids) == sum(node_counts.values()),
    'all_edge_ids_unique': len(edge_ids) == sum(edge_counts.values()),
}
print(json.dumps(summary, indent=2))
assert {'AST', 'CFG', 'DFG', 'CALL'} <= set(edge_counts)
assert summary['all_node_ids_unique'] and summary['all_edge_ids_unique']
print('PASS: all CPG categories exist and IDs are unique')"""
    tests_source = """import subprocess
import sys
from pathlib import Path

result = subprocess.run(
    [sys.executable, '-m', 'pytest', '-q'],
    cwd=Path('..').resolve(), capture_output=True, text=True, check=True,
)
print(result.stdout.rstrip())
assert '[100%]' in result.stdout
print('PASS: parser, replay, schema, and syntax-error tests')"""
    write(
        "task2_parser.ipynb",
        notebook(
            "Task 2 - Incremental CPG Parser Service",
            """The service uses Python `ast`, stable structural IDs,
statement-level CFG, lexical reaching-definitions DFG, and conservative
top-level same-file call resolution. It releases each file graph before
processing the next file.

```mermaid
flowchart LR
  F[One Python file] --> A[Python ast]
  A --> N[AST nodes and edges]
  A --> C[Statement CFG]
  C --> D[Reaching-definitions DFG]
  A --> K[Conservative CALL edges]
  N --> T[One Kafka transaction]
  C --> T
  D --> T
  K --> T
  T --> M[Manifest advances after commit]
```

## Approach and rationale

**Approach:** Each file is decoded, analyzed, reconciled against its previous
stable node and edge IDs, and emitted in one Kafka transaction. AST field/index
paths define identity; the CFG models statement flow; DFG uses a bounded
per-scope fixed point; unresolved definitions and calls become explicit external
nodes rather than guessed targets.

**Why this approach:** The standard-library AST is available without a heavy
Joern runtime and preserves every Python syntax node needed by the lab.
File-local passes bound graph memory by the largest source file. Structural IDs,
stale deletes, and updating SQLite only after Kafka commits jointly make retries
idempotent and recoverable.

**Alternatives and trade-offs:** Joern offers deeper interprocedural semantics
and tree-sitter offers robust multi-version parsing, but both add integration
cost beyond the laboratory scope. The chosen analysis deliberately sacrifices
alias analysis, precise exception flow, and dynamic dispatch; warnings and
external nodes expose those limits instead of overstating accuracy.""",
            [code_cell(aggregate_source), code_cell(tests_source)],
            """**Worked:** Deterministic IDs, all four CPG edge categories, bounded file-by-file processing, and stale-element reconciliation pass the regression suite.

**Failed:** Syntax errors originally risked leaving the last valid graph in Neo4j, while attribute calls and `AugAssign`/`del` produced misleading static-analysis results.

**Resolution:** Error transactions now delete stale elements and update the manifest; dynamic attribute calls remain external, and DFG transfer functions explicitly model augmented reads and deletion kills. Aliasing and runtime dispatch remain documented limits.""",
        ),
    )

def build_task3() -> None:
    topics_source = """import subprocess
from pathlib import Path

root = Path('..').resolve()
topics = [
    'cpg.nodes.v1', 'cpg.edges.v1', 'cpg.source-metadata.v1',
    'cpg.parser-errors.v1', 'cpg.neo4j-dlq.v1',
]
for topic in topics:
    result = subprocess.run(
        ['docker', 'compose', 'exec', '-T', 'broker', 'kafka-topics',
         '--bootstrap-server', 'broker:29092', '--describe', '--topic', topic],
        cwd=root, capture_output=True, text=True, check=True,
    )
    print(result.stdout.rstrip())
print('PASS: four required topics and the connector DLQ are configured')"""
    samples_source = """import json
from pathlib import Path

root = Path('..').resolve()
evidence = json.loads((root / 'evidence/runtime/verification.json').read_text(encoding='utf-8'))
samples = evidence['kafka_samples']
required = {'cpg.nodes.v1', 'cpg.edges.v1', 'cpg.source-metadata.v1', 'cpg.parser-errors.v1'}
assert required <= set(samples)
for record in samples.values():
    assert record['source'].startswith('Kafka broker')
    assert record['key']
    assert record['value']['schema_version'] == '1.0'
    assert record['value']['event_time'].endswith('Z')
print(json.dumps(samples, indent=2, ensure_ascii=False))
print('PASS: read_committed broker samples cover all four required topics')"""
    write(
        "task3_kafka.ipynb",
        notebook(
            "Task 3 - Kafka topic and event design",
            """Nodes, edges, metadata, and parser failures have separate topics.
Every record carries a schema version, event time, stable key, content hash,
run ID, event ID, and operation.

```mermaid
flowchart TB
  P[Parser Service] -->|node_id key| N[cpg.nodes.v1 compact]
  P -->|edge_id key| E[cpg.edges.v1 compact]
  P -->|file_id key| M[cpg.source-metadata.v1 compact]
  P -->|error_id key| X[cpg.parser-errors.v1 delete retention]
  N --> C[Neo4j Kafka Connect]
  E --> C
  M --> S[Spark Structured Streaming]
```

## Approach and rationale

**Approach:** The four required event families use separate, explicitly created
topics. Node, edge, and metadata records use stable entity keys and compaction;
parser errors use time-based delete retention. Every JSON envelope carries
versioning, UTC event time, repository/file/run identity, content hash, and an
operation.

**Why this approach:** Neo4j and Spark need different schemas, retention, and
failure handling. Stable keys let compaction and downstream `MERGE`/upsert
converge on the latest entity state, while retaining parser errors preserves an
audit trail without treating an error as graph state.

**Alternatives and trade-offs:** A single multiplexed topic would simplify topic
creation but force every consumer to filter unrelated records and weaken schema
isolation. A Schema Registry would provide stronger centralized governance; for
this self-contained lab, versioned JSON Schemas and contract tests keep setup
smaller. One partition per topic favors deterministic replay, while cross-topic
ordering is intentionally handled by idempotent sinks.""",
            [code_cell(topics_source), code_cell(samples_source)],
            """**Worked:** All required topics have the intended partition, replication, cleanup policy, keyed records, schema version, and UTC event time.

**Failed:** In-memory samples proved serialization but did not prove that Kafka had accepted and exposed the records.

**Resolution:** Replay capture now publishes an invalid fixture and stores `read_committed` samples consumed from the live broker for node, edge, metadata, and parser-error topics. Compaction is combined with stable keys and idempotent sinks.""",
        ),
    )

def build_task4() -> None:
    status_source = """import json
import subprocess
from pathlib import Path

root = Path('..').resolve()
result = subprocess.run(
    ['docker', 'compose', 'exec', '-T', 'connect', 'curl', '-fsS',
     'http://localhost:8083/connectors/cpg-neo4j-sink/status'],
    cwd=root, capture_output=True, text=True, check=True,
)
status = json.loads(result.stdout)
print(json.dumps(status, indent=2))
assert status['connector']['state'] == 'RUNNING'
assert status['tasks'] and all(task['state'] == 'RUNNING' for task in status['tasks'])
print('PASS: connector and task are RUNNING')"""
    counts_source = """import json
import sys
from pathlib import Path

root = Path('..').resolve()
sys.path.insert(0, str(root / 'scripts'))
from capture_replay_evidence import neo4j_snapshot

evidence = json.loads((root / 'evidence/runtime/verification.json').read_text(encoding='utf-8'))
file_id = evidence['replay_file']['file_id']
snapshot = neo4j_snapshot(file_id)
print(json.dumps(snapshot, indent=2))
assert snapshot['nodes'] == snapshot['unique_nodes']
assert snapshot['edges'] == snapshot['unique_edges']
assert {'AST', 'CFG', 'DFG', 'CALL'} <= set(snapshot['edge_kinds'])
print('PASS: Neo4j IDs are unique and all graph edge kinds exist')"""
    dlq_source = """import subprocess
from pathlib import Path

root = Path('..').resolve()
result = subprocess.run(
    ['docker', 'compose', 'exec', '-T', 'broker', 'kafka-get-offsets',
     '--bootstrap-server', 'broker:29092', '--topic', 'cpg.neo4j-dlq.v1'],
    cwd=root, capture_output=True, text=True, check=True,
)
print(result.stdout.rstrip())
offset = sum(int(line.rsplit(':', 1)[1]) for line in result.stdout.splitlines() if ':' in line)
assert offset == 0
print('PASS: Neo4j connector DLQ is empty')"""
    write(
        "task4_neo4j.ipynb",
        notebook(
            "Task 4 - Graph topology ingestion into Neo4j",
            """Kafka Connect consumes only node and edge topics. Cypher handlers
use `MERGE`, create placeholder endpoints when necessary, and reconcile delete
events without Spark.

## Approach and rationale

**Approach:** A dedicated Neo4j Kafka Sink subscribes only to
`cpg.nodes.v1` and `cpg.edges.v1`. One generic `CPGNode` label and one
`CPG_EDGE` relationship type carry semantic kinds as properties. Cypher
handlers `MERGE` by stable ID, create missing endpoints for out-of-order edge
arrival, and process explicit delete events. A uniqueness constraint protects
node IDs and the connector routes failures to a DLQ.

**Why this approach:** Direct Kafka Connect ingestion is an explicit assignment
constraint and avoids making Spark a graph relay. Property-based kinds keep the
Cypher handlers static and compatible, while placeholder endpoints remove any
false assumption that separate node and edge topics arrive in lockstep.

**Alternatives and trade-offs:** Dynamic Neo4j labels and relationship types
would look more natural in Browser queries but complicate parameterized sink
Cypher and schema evolution. Waiting for every node before consuming edges
would require cross-topic coordination; placeholders plus later node `MERGE`
provide eventual convergence with a simpler, retry-safe connector.""",
            [
                code_cell(status_source),
                code_cell(counts_source),
                code_cell(dlq_source),
                image_cell("neo4j-browser.png", "Neo4j Browser showing CPG nodes and relationships"),
            ],
            """**Worked:** Connector/task status, total-versus-distinct IDs, all edge kinds, the Browser graph, and an empty DLQ independently confirm direct graph ingestion.

**Failed:** Connector registration initially raced the asynchronous REST API and could finish before the task reached `RUNNING`.

**Resolution:** Registration is idempotent and followed by bounded status polling; edge Cypher creates placeholder endpoints so cross-topic arrival order cannot lose relationships.""",
        ),
    )

def build_task5() -> None:
    mongo_source = f"""import json
import sys
from pathlib import Path

root = Path('..').resolve()
sys.path.insert(0, str(root / 'scripts'))
from capture_replay_evidence import mongo_snapshot

snapshot = mongo_snapshot('{REPLAY_FILE_ID}')
print(json.dumps(snapshot, indent=2))
assert snapshot['documents'] == snapshot['distinct_files'] == 61
assert snapshot['document']['_id'] == '{REPLAY_FILE_ID}'
print('PASS: MongoDB has one replacement-upserted document per file')"""
    checkpoint_source = """import json
import sys
from pathlib import Path

root = Path('..').resolve()
sys.path.insert(0, str(root / 'scripts'))
from capture_replay_evidence import checkpoint_offset, kafka_end_offset

progress = {
    'spark_checkpoint_offset': checkpoint_offset(),
    'metadata_topic_end_offset': kafka_end_offset('cpg.source-metadata.v1'),
}
print(json.dumps(progress, indent=2))
assert progress['spark_checkpoint_offset'] == progress['metadata_topic_end_offset']
print('PASS: Spark checkpoint has consumed the metadata topic')"""
    write(
        "task5_mongodb.ipynb",
        notebook(
            "Task 5 - Source metadata ingestion into MongoDB",
            """Spark reads only `cpg.source-metadata.v1`, parses an explicit
schema, and writes replacement upserts keyed by `_id=file_id`. Its checkpoint
is retained on a Docker volume.

## Approach and rationale

**Approach:** One Spark Structured Streaming query reads the metadata topic with
an explicit nested schema and `read_committed` isolation. The MongoDB connector
uses replacement upserts with `_id=file_id`; Kafka offsets are stored in each
document as evidence, while Spark recovery state lives in a persistent
checkpoint volume.

**Why this approach:** Metadata is naturally one document per source file, so a
replacement upsert expresses the desired latest-state model and prevents field
fragments from surviving an update. An explicit schema surfaces incompatible
events early. The checkpoint, rather than an application-maintained offset,
lets Spark resume with its own checkpointed offset and progress protocol.

**Alternatives and trade-offs:** Append-only MongoDB writes would retain event
history but violate the required no-duplication final state. `foreachBatch`
could implement custom writes, yet the MongoDB Spark Connector already supplies
the required sink semantics with less custom code. `startingOffsets=earliest`
is useful only for an empty checkpoint; preserving the volume is what prevents
old offsets from being replayed after restart.""",
            [
                code_cell(mongo_source),
                code_cell(checkpoint_source),
                image_cell(
                    "mongodb-ui.png",
                    "MongoDB UI capture of the replay file; the executable output above verifies the final live offset and checkpoint",
                ),
            ],
            """**Worked:** Spark consumes only metadata, MongoDB maintains one `_id=file_id` document per source file, and the checkpoint reaches the Kafka end offset.

**Failed:** First startup was slow while resolving connector packages, and document counts alone could not distinguish replacement from duplication.

**Resolution:** A persistent Ivy cache and checkpoint volume support restart, while distinct-ID counts, content hashes, Kafka offsets, and replacement-upsert settings verify the sink behavior.""",
        ),
    )

def build_task6() -> None:
    evidence = load_evidence()
    stages = evidence["stages"]
    labels = {
        "baseline": "Locked baseline",
        "modified": "Modified file",
        "forced_unchanged": "Forced unchanged replay",
        "restart_replay": "Spark restart + replay",
    }
    rows = []
    for key in ("baseline", "modified", "forced_unchanged", "restart_replay"):
        stage = stages[key]
        rows.append(
            f"| {labels[key]} | `{stage['source_hash'][:12]}` | "
            f"{stage['neo4j']['file_nodes']} | {stage['neo4j']['file_edges']} | "
            f"{stage['neo4j']['nodes']} | {stage['neo4j']['edges']} | "
            f"{stage['mongo']['documents']} | {stage['mongo']['document']['kafka_offset']} | "
            f"{stage['spark_checkpoint_offset']} | PASS |"
        )
    table = """| Stage | Hash | File nodes | File edges | Total nodes | Total edges | Mongo docs | Mongo offset | Checkpoint | Result |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
""" + "\n".join(rows)
    intro = f"""The replay run is captured as four ordered stages from one automation script.

{table}

```mermaid
sequenceDiagram
  participant P as Parser
  participant K as Kafka
  participant N as Neo4j
  participant S as Spark checkpoint
  participant M as MongoDB
  P->>K: Locked baseline file
  K->>N: Replace graph state
  K->>S: Consume metadata offset
  S->>M: Replace file document
  P->>K: Modified file only
  P->>K: Force same modified bytes
  Note over N,M: Counts and distinct IDs must remain stable
  S-->>S: Restart with checkpoint preserved
  P->>K: One post-restart replay
  Note over S: First input starts at saved offset
```

## Approach and rationale

**Approach:** One automation run establishes a locked baseline, applies the
seven-line modification to exactly `optimum/version.py`, forces an unchanged
replay, restarts Spark without deleting its checkpoint, and then publishes one
post-restart replay. Each stage waits until Kafka, Neo4j, MongoDB, and the Spark
checkpoint converge before taking a snapshot.

**Why this approach:** The modified stage proves that stale graph elements are
removed and new content is visible. The forced-unchanged stage isolates
idempotency from the parser's normal hash-based skip. The restart stage proves
recovery semantics: an unchanged checkpoint and MongoDB snapshot while idle,
followed by a first input batch whose start offset equals the saved checkpoint,
is stronger evidence than merely showing a larger final offset.

**Alternatives and trade-offs:** Checking only total counts could hide duplicate
IDs or rewrites of unrelated files, so the assertions also compare distinct
IDs, content hashes, per-file counts, offsets, and a digest of the other 60
MongoDB documents. The script temporarily restores the locked bytes and restores
the modified bytes in a `finally` block; this adds orchestration complexity but
keeps one reproducible diff while protecting the demonstration worktree."""
    evidence_source = """import json
from pathlib import Path

root = Path('..').resolve()
evidence = json.loads((root / 'evidence/runtime/verification.json').read_text(encoding='utf-8'))
summary = {
    'repository': evidence['repository'],
    'replay_file': {k: v for k, v in evidence['replay_file'].items() if k != 'git_diff'},
    'stages': {
        name: {
            'source_hash': stage['source_hash'],
            'parser': stage['parser'],
            'neo4j': stage['neo4j'],
            'mongo': stage['mongo'],
            'kafka_metadata_end_offset': stage['kafka_metadata_end_offset'],
            'spark_checkpoint_offset': stage['spark_checkpoint_offset'],
        }
        for name, stage in evidence['stages'].items()
    },
    'spark_restart': evidence['spark_restart'],
    'neo4j_dlq_end_offset': evidence['neo4j_dlq_end_offset'],
    'assertions': evidence['assertions'],
}
print(json.dumps(summary, indent=2, ensure_ascii=False))
assert evidence['assertions'] and all(evidence['assertions'].values())
print('PASS: every captured replay assertion is true')"""
    diff_source = f"""import subprocess
from pathlib import Path

root = Path('..').resolve()
repo = root / 'source-repo'
if not repo.is_dir():
    repo = root.parent / 'source-repo'
result = subprocess.run(
    ['git', '-C', str(repo), 'diff', '--', '{REPLAY_FILE}'],
    capture_output=True, text=True, check=True,
)
print(result.stdout.rstrip())
assert 'lab04_replay_probe' in result.stdout
print('PASS: replay modification is present in exactly {REPLAY_FILE}')"""
    live_source = f"""import json
import sys
from pathlib import Path

root = Path('..').resolve()
sys.path.insert(0, str(root / 'scripts'))
from capture_replay_evidence import checkpoint_offset, kafka_end_offset, mongo_snapshot, neo4j_snapshot

evidence = json.loads((root / 'evidence/runtime/verification.json').read_text(encoding='utf-8'))
expected = evidence['stages']['restart_replay']
live = {{
    'neo4j': neo4j_snapshot('{REPLAY_FILE_ID}'),
    'mongo': mongo_snapshot('{REPLAY_FILE_ID}'),
    'kafka_metadata_end_offset': kafka_end_offset('cpg.source-metadata.v1'),
    'spark_checkpoint_offset': checkpoint_offset(),
}}
print(json.dumps(live, indent=2))
assert live['neo4j'] == expected['neo4j']
assert live['mongo'] == expected['mongo']
assert live['kafka_metadata_end_offset'] == expected['kafka_metadata_end_offset']
assert live['spark_checkpoint_offset'] == expected['spark_checkpoint_offset']
print('PASS: live final state matches the captured restart-replay stage')"""
    write(
        "task6_replay.ipynb",
        notebook(
            "Task 6 - Idempotent replay verification",
            intro,
            [code_cell(evidence_source), code_cell(diff_source), code_cell(live_source)],
            """**Worked:** Modified and forced-unchanged replays converge to unique Neo4j IDs and one updated MongoDB document without changing the other 60 documents.

**Failed:** Before/after checkpoint numbers alone did not prove that the restarted Spark query skipped previously committed offsets.

**Resolution:** The Spark listener now records the restarted query run and its first input batch; the evidence verifies that the batch starts exactly at the saved checkpoint, while an idle pre-publish snapshot proves MongoDB was unchanged.""",
        ),
    )

def main() -> None:
    names = [
        "architecture.ipynb",
        "task1_repository.ipynb",
        "task2_parser.ipynb",
        "task3_kafka.ipynb",
        "task4_neo4j.ipynb",
        "task5_mongodb.ipynb",
        "task6_replay.ipynb",
    ]
    load_evidence()
    for image in ("neo4j-browser.png", "mongodb-ui.png"):
        if not (BOOK / "figures" / image).is_file():
            raise RuntimeError(f"Missing required database UI capture: book/figures/{image}")
    build_architecture()
    build_task1()
    build_task2()
    build_task3()
    build_task4()
    build_task5()
    build_task6()
    execute_notebooks(names)
    print("Generated and executed seven notebooks with nbclient in book/")


if __name__ == "__main__":
    main()
