"""DAG files must orchestrate, not compute (plan/08 section 1).

An AST check, so it needs no Airflow installed and cannot be defeated by the import
succeeding. The rule it enforces: a DAG file may call into `fleet.*` and shell out to
job modules, but it may not do the work itself.

The reason is not style. Logic that lives in a DAG file is logic that only runs under
a scheduler, so it is not covered by the unit suite and not shared with the speed
layer. It becomes a second, untested copy of the pipeline that drifts from the first -
which is precisely the "managing multiple codebases" criticism Lambda is charged with,
reproduced inside our own orchestration layer.

This mirrors `test_transforms_purity.py`, which enforces the same separation between
transforms and I/O.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

DAG_DIR = Path(__file__).resolve().parents[2] / "airflow/dags"

FORBIDDEN_IMPORTS = {"pandas", "numpy", "pyspark", "sklearn"}

# Method names that mean an aggregation is happening in the DAG file.
FORBIDDEN_CALLS = {"groupby", "agg", "merge", "pivot_table", "resample", "rolling"}


def _dag_files() -> list[Path]:
    return sorted(DAG_DIR.glob("*.py"))


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


def test_there_is_at_least_one_dag_file():
    assert _dag_files(), f"no DAG files found in {DAG_DIR}"


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_dag_file_parses(path):
    _tree(path)


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_no_dataframe_libraries_imported(path):
    found = set()
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Import):
            found |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module.split(".")[0])
    offending = found & FORBIDDEN_IMPORTS
    assert not offending, (
        f"{path.name} imports {sorted(offending)} - computation belongs in fleet.*, "
        "which the unit suite covers and both layers share"
    )


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_no_aggregation_calls(path):
    offending = []
    for node in ast.walk(_tree(path)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in FORBIDDEN_CALLS
        ):
            offending.append(f"{node.func.attr}() at line {node.lineno}")
    assert not offending, f"{path.name} aggregates in the DAG file: {offending}"


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_no_sql_in_the_dag_file(path):
    """SQL belongs in store/mart.py, beside the statements that write the same tables.

    Split across two files, a column rename breaks one half silently.
    """
    offending = []
    for node in ast.walk(_tree(path)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            upper = node.value.upper()
            if any(
                kw in upper
                for kw in ("SELECT ", "INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE TABLE")
            ):
                offending.append(f"line {node.lineno}: {node.value[:60]!r}")
    assert not offending, f"{path.name} embeds SQL: {offending}"


@pytest.mark.parametrize("path", _dag_files(), ids=lambda p: p.name)
def test_every_dag_file_documents_itself(path):
    """These files are read by an examiner, and by whoever is on call at 3am."""
    doc = ast.get_docstring(_tree(path))
    assert doc and len(doc) > 200, f"{path.name} needs a module docstring explaining the flow"
