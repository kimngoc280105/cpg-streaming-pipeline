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

## Why the pipeline is split

The graph and metadata paths solve different problems. Neo4j needs individual
nodes and relationships, whereas MongoDB needs one current document for each
source file. Keeping those contracts separate at Kafka lets the Neo4j connector
consume topology directly and leaves Spark responsible only for metadata and
its checkpoint. It also means that a parser failure or a bad connector record
has its own visible error path instead of blocking valid traffic.

We considered sending every event through one Spark job. That would reduce the
number of arrows in the diagram, but it would also couple two unrelated schemas
and make Spark an unnecessary relay for Neo4j, contrary to the assignment. The
single broker, one partition per topic, and replication factor one are deliberate
demo choices: they make ordering and replay easier to inspect, but they are not
presented as a production deployment."""
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
            """The final run confirmed that topology reaches Neo4j without
passing through Spark, while metadata advances independently through the Spark
checkpoint. The first connector startup exposed an integration race: Kafka
Connect accepted the registration request before its task was ready, so an
immediate status check sometimes returned 404. `register-wait.sh` now retries
until both the connector and task report `RUNNING`. The remaining limitation is
intentional and visible in the diagram: this is a single-node teaching stack,
not a highly available Kafka deployment.""",
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

## Reproducible repository scope

The bootstrap script uses a depth-one clone and then checks out the recorded
Optimum commit. Pinning matters more than simply recording the URL: if the
default branch moved, both the discovered-file count and every downstream CPG
count could change. The report therefore keeps the locked baseline separate
from the seven lines added later for the replay demonstration.

Discovery reports two numbers on purpose. The raw count shows every Python file
that exists at the selected commit; the processed count applies the documented
test, setup, build, and generated-file exclusions. This makes the optional
filtering auditable. Vendoring the full third-party tree would make the
submission larger without improving reproducibility, while processing every
generated or test file would increase runtime and obscure the chosen scope.""",
            [code_cell(source)],
            f"""The locked clone produced {repository['raw_python_files']} raw
Python files and {repository['processed_python_files']} files after filtering,
and every processed file parsed successfully. An early version relied on the
current default branch, which made the recorded counts vulnerable to upstream
changes; the replay edit also made a single “total lines” number ambiguous.
Fetching the recorded commit fixed the first problem, and reporting baseline
and modified line counts separately fixed the second. The exclusion list remains
part of the output so that another team can reproduce exactly the same scope.""",
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

## Parser strategy and its limits

Python's standard `ast` module was the pragmatic choice for this repository: it
preserves every syntax node required by the lab and introduces no separate
runtime. Joern could provide deeper interprocedural analysis, and tree-sitter
would be attractive for mixed Python versions, but either choice would add
integration work without removing the need to define stable IDs and replay
semantics ourselves.

Incrementality is enforced at the file boundary. A file is decoded, analyzed,
compared with its previous stable node and edge sets, and published in one Kafka
transaction before the SQLite manifest advances. This order is important:
updating the manifest first could make a failed Kafka transaction look complete.
Processing and then releasing one graph bounds analysis memory by the largest
file rather than by the repository.

The semantic passes are deliberately conservative. Structural AST paths define
identity, CFG edges represent statement-level control, and a bounded
reaching-definitions fixed point supplies DFG edges within each lexical scope.
When a definition or call target cannot be proved locally, the graph records an
external node instead of guessing. Alias analysis, precise exception dispatch,
and dynamic method resolution remain explicit limitations.""",
            [code_cell(aggregate_source), code_cell(tests_source)],
            """The regression run produced deterministic IDs and all four CPG
edge categories while processing files one at a time. Two weaknesses surfaced
during fixture testing. A syntax error could leave the previous valid graph
visible, and the first DFG implementation mishandled augmented assignments and
deletions. Error transactions now remove stale topology before publishing error
metadata, while the transfer functions model the implicit read in `x += value`
and the kill caused by `del`. Attribute calls still remain external because
resolving them without type information would create more misleading edges than
useful ones.""",
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

## Topic boundaries and replay semantics

The four required event families are separated because their consumers and
retention needs are different. Neo4j subscribes to nodes and edges, Spark reads
metadata, and parser failures remain available for diagnosis without being
treated as graph state. Node, edge, and metadata topics use stable entity keys
with compaction; the error topic instead uses time-based retention.

Every JSON envelope repeats the information needed to interpret it independently:
schema version, UTC event time, repository and file identity, run ID, content
hash, and operation. Stable keys are what allow compaction, Neo4j `MERGE`, and
MongoDB replacement upserts to converge after replay. A single multiplexed
topic would be easier to create but would force both downstream systems to
filter unrelated schemas.

For a larger platform we would prefer a Schema Registry and multiple partitions.
Here, versioned JSON Schemas and contract tests keep the environment
self-contained, while one partition per topic makes the replay sequence easy to
inspect. There is still no assumed ordering between different topics; the sink
design handles that explicitly.""",
            [code_cell(topics_source), code_cell(samples_source)],
            """Topic inspection confirmed the intended partition count,
replication factor, cleanup policies, keys, schema versions, and UTC timestamps.
Our first evidence consisted of records captured before publication, which only
proved that Python could serialize them. The replay script now publishes an
invalid fixture and then consumes `read_committed` samples back from the live
broker for all four required topics. That change turned the chapter from a
contract demonstration into evidence that Kafka actually accepted and exposed
the records.""",
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

## Direct graph ingestion

The Neo4j sink subscribes only to `cpg.nodes.v1` and `cpg.edges.v1`; metadata
never enters this branch. That boundary is both an assignment requirement and a
useful design constraint, because it prevents Spark from becoming a graph relay.
Stable IDs are used in Cypher `MERGE` operations, explicit delete events remove
obsolete topology, and a uniqueness constraint provides a second guard against
duplicate nodes.

Node and edge topics do not share a global arrival order. Rather than delaying
all edges until a separate coordination step proves every endpoint exists, the
edge handler creates placeholder `CPGNode` endpoints. A later node event merges
properties into the same ID. This makes retries and cross-topic ordering
converge without an external buffer.

We kept one generic node label and relationship type, with AST/CFG/DFG/CALL
kinds stored as properties. Dynamic labels would produce prettier Browser
queries, but they complicate parameterized connector Cypher and schema changes.
Failed records go to a dedicated DLQ so that connector tolerance cannot silently
hide ingestion errors.""",
            [
                code_cell(status_source),
                code_cell(counts_source),
                code_cell(dlq_source),
                image_cell("neo4j-browser.png", "Neo4j Browser showing CPG nodes and relationships"),
            ],
            """The connector and its task remained `RUNNING`, total counts
matched distinct IDs, every graph edge kind was present, and the DLQ stayed
empty. The Neo4j Browser image provides a separate visual check of the resulting
topology. The main failure occurred during registration: the REST request could
return before the asynchronous task reached a runnable state. Registration is
now idempotent and followed by bounded polling, which distinguishes “request
accepted” from “sink ready” and makes repeated stack startup reliable.""",
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

## Why metadata goes through Spark

Metadata is naturally modeled as one current document per source file, unlike
the many nodes and edges stored in Neo4j. The streaming query therefore reads
only `cpg.source-metadata.v1`, applies an explicit nested schema, and uses the
MongoDB connector to replace/upsert `_id=file_id`. Replacement is important:
an append-only collection would preserve history but would fail the lab's
no-duplication final-state requirement, while partial updates could leave stale
fields behind.

Spark owns progress through a persistent checkpoint volume instead of through a
custom offset table. `startingOffsets=earliest` matters only when that checkpoint
is empty; after the first run, the saved offsets decide where processing resumes.
Each MongoDB document also records its Kafka offset, which makes the relationship
between source progress and the visible document inspectable.

A custom `foreachBatch` writer could implement the same policy, but the official
MongoDB Spark Connector already provides replacement upserts and reduces the
amount of recovery code we would have to maintain. The explicit schema and
`read_committed` isolation trade some flexibility for earlier detection of
incompatible or uncommitted events.""",
            [
                code_cell(mongo_source),
                code_cell(checkpoint_source),
                image_cell(
                    "mongodb-ui.png",
                    "MongoDB UI capture of the replay file; the executable output above verifies the final live offset and checkpoint",
                ),
            ],
            """The final collection contains one `_id=file_id` document for
each processed source file, and the checkpoint reaches the metadata topic's end
offset. First startup was noticeably slower because Spark had to resolve the
Kafka and MongoDB packages; persisting the Ivy cache removed that repeated cost.
We also learned that a document count alone is weak evidence: the same count can
hide accidental rewrites or stale content. The chapter now pairs distinct-ID
counts with the replay file's content hash, Kafka offset, and checkpoint so the
replacement behavior can be verified rather than inferred.""",
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

## What the four stages prove

The baseline first restores the locked version of `optimum/version.py`; without
that reference point, a later count difference would have no reproducible
meaning. The modified stage then adds the seven-line probe and processes only
that file, demonstrating both stale-element deletion and the new graph state.
Next, the same modified bytes are forced through the parser again. This stage is
necessary because the normal content-hash shortcut would otherwise skip the
file and never exercise downstream idempotency.

The final stage restarts Spark without removing its checkpoint. Before another
record is published, the automation records that both the checkpoint and the
MongoDB state remain unchanged. It then sends one metadata event and inspects
the restarted query's first input batch. A start offset equal to the saved
checkpoint is stronger evidence of correct recovery than simply showing that
the final offset increased.

Counts alone are not enough for this argument: duplicates can leave totals
plausible, and an implementation could rewrite unrelated MongoDB documents.
The assertions therefore compare total and distinct IDs, per-file counts,
content hashes, Kafka offsets, and a digest of the other 60 documents. The
script temporarily swaps the baseline and modified bytes, then restores the
demonstration version in a `finally` block so a failed run cannot silently
destroy the prepared worktree."""
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
            """Both the modified and forced-unchanged stages converged to
unique Neo4j IDs and one current MongoDB document, while the digest of the other
60 documents stayed unchanged. The first version of this experiment compared
only checkpoint numbers before and after restart. That showed progress, but it
did not prove where the new Spark query began reading. A progress listener now
captures the restarted query ID and first non-empty batch; its start offset
matches the saved checkpoint. Combined with the idle pre-publish snapshot, this
closes the gap between “the numbers increased” and evidence that previously
committed offsets were actually skipped.""",
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
