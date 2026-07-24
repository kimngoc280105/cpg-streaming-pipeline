from pathlib import Path

from cpg_parser.analyzer import CPGAnalyzer


FIXTURE = Path(__file__).parent / "fixtures" / "sample_repo" / "app.py"


def analyze():
    source = FIXTURE.read_text(encoding="utf-8")
    return CPGAnalyzer(source, "f" * 64, "app.py").analyze()


def test_ids_are_deterministic():
    first = analyze()
    second = analyze()
    assert {node.id for node in first.nodes} == {node.id for node in second.nodes}
    assert {edge.id for edge in first.edges} == {edge.id for edge in second.edges}


def test_all_required_graph_categories_exist():
    result = analyze()
    assert {"AST", "CFG", "DFG", "CALL"} <= {edge.kind for edge in result.edges}
    assert any(edge.discriminator == "if-true" for edge in result.edges if edge.kind == "CFG")
    assert any(edge.discriminator == "loop-body" for edge in result.edges if edge.kind == "CFG")
    assert any(edge.variable == "total" for edge in result.edges if edge.kind == "DFG")


def test_call_resolution_has_internal_and_external_targets():
    result = analyze()
    call_edges = [edge for edge in result.edges if edge.kind == "CALL"]
    assert any(edge.resolved for edge in call_edges)
    assert any(not edge.resolved for edge in call_edges)


def test_attribute_call_is_not_resolved_by_unrelated_method_name():
    source = """
class Worker:
    def run(self):
        return 1

def invoke(obj):
    return obj.run()
"""
    result = CPGAnalyzer(source, "f" * 64, "dynamic.py").analyze()
    call = next(edge for edge in result.edges if edge.kind == "CALL")
    assert call.discriminator == "obj.run"
    assert not call.resolved


def test_augassign_reads_previous_definition_and_then_defines_target():
    source = "def f(x):\n    x += 1\n    return x\n"
    result = CPGAnalyzer(source, "f" * 64, "augassign.py").analyze()
    nodes = {node.id: node for node in result.nodes}
    x_edges = [edge for edge in result.edges if edge.kind == "DFG" and edge.variable == "x"]

    assert any(
        nodes[edge.source_id].ast_type == "arg"
        and nodes[edge.target_id].structural_path.endswith(".body[0].target")
        for edge in x_edges
    )
    assert any(
        nodes[edge.source_id].structural_path.endswith(".body[0].target")
        and nodes[edge.target_id].line == 3
        for edge in x_edges
    )


def test_delete_kills_reaching_definition():
    source = "def f(x):\n    del x\n    return x\n"
    result = CPGAnalyzer(source, "f" * 64, "delete.py").analyze()
    nodes = {node.id: node for node in result.nodes}
    return_use = next(node for node in result.nodes if node.ast_type == "Name" and node.line == 3)
    incoming = [
        edge for edge in result.edges if edge.kind == "DFG" and edge.target_id == return_use.id
    ]

    assert len(incoming) == 1
    assert not incoming[0].resolved
    assert nodes[incoming[0].source_id].ast_type == "ExternalSymbol"
