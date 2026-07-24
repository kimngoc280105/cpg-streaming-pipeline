from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict
import nbformat

GENERATED: Dict[str, nbformat.NotebookNode] = {}


def load_evidence() -> Dict[str, Any]:
    return {}


def _create_dummy_notebook(title: str) -> nbformat.NotebookNode:
    nb = nbformat.v4.new_notebook()
    cell1 = nbformat.v4.new_markdown_cell(f"# {title}")
    cell2 = nbformat.v4.new_code_cell("print('Hello World')")
    cell2.execution_count = None
    cell2.outputs = []
    nb.cells = [cell1, cell2]
    return nb


def build_architecture():
    GENERATED["architecture"] = _create_dummy_notebook("Architecture")


def build_task1():
    GENERATED["task1"] = _create_dummy_notebook("Task 1")


def build_task2():
    GENERATED["task2"] = _create_dummy_notebook("Task 2")


def build_task3():
    GENERATED["task3"] = _create_dummy_notebook("Task 3")


def build_task4():
    GENERATED["task4"] = _create_dummy_notebook("Task 4")


def build_task5():
    GENERATED["task5"] = _create_dummy_notebook("Task 5")


def build_task6():
    GENERATED["task6"] = _create_dummy_notebook("Task 6")


def main():
    build_architecture()
    build_task1()
    build_task2()
    build_task3()
    build_task4()
    build_task5()
    build_task6()
    print(f"Generated {len(GENERATED)} notebooks.")


if __name__ == "__main__":
    main()
