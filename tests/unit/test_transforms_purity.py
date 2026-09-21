"""The architectural invariant that makes the Lambda argument true.

`fleet.transforms` must contain nothing but pure DataFrame -> DataFrame logic. The
moment it imports a Kafka client or a database driver, it stops being shareable
between the speed layer and the batch layer, and the answer to the module's
"managing multiple codebases" criticism collapses.

Enforced here rather than by convention, because conventions erode.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

TRANSFORMS = Path(__file__).resolve().parents[2] / "src" / "fleet" / "transforms"
MODULES = sorted(p for p in TRANSFORMS.glob("*.py") if p.name != "__init__.py")

FORBIDDEN_PREFIXES = (
    "redis",
    "psycopg",
    "boto3",
    "botocore",
    "confluent_kafka",
    "kafka",
    "pyspark.sql.streaming",
    "requests",
)


def imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def test_transforms_package_is_not_empty():
    assert MODULES, "no transform modules found - the shared package is the whole point"


@pytest.mark.parametrize("module", MODULES, ids=lambda p: p.name)
def test_no_io_dependencies(module):
    """A transform that talks to a store cannot be shared between layers."""
    offending = {
        name
        for name in imported_modules(module)
        if any(name == p or name.startswith(p + ".") for p in FORBIDDEN_PREFIXES)
    }
    assert not offending, f"{module.name} imports I/O: {sorted(offending)}"


@pytest.mark.parametrize("module", MODULES, ids=lambda p: p.name)
def test_no_streaming_specific_calls(module):
    """`readStream`/`writeStream` belong to an entry point, not to shared logic.

    `withWatermark` is deliberately allowed: it is declarative, is a no-op on a
    static DataFrame, and the batch layer benefits from the same declaration.
    """
    source = module.read_text()
    for banned in ("readStream", "writeStream", "foreachBatch", "awaitTermination"):
        assert banned not in source, f"{module.name} contains {banned}"


@pytest.mark.parametrize("layer", ["speed_layer", "batch_layer"])
def test_layer_imports_from_transforms(layer):
    """The claim under test: the SAME package feeds both entry points.

    Checked per layer and skipped for one not yet implemented, so the suite stays
    honest about what has actually been built rather than failing on absence. Once
    a layer exists it MUST import the shared package - if either one grows its own
    copy of the business logic, the answer to the module's "managing multiple
    codebases" criticism is gone.
    """
    root = Path(__file__).resolve().parents[2] / "src" / "fleet" / layer
    modules = [p for p in root.rglob("*.py") if p.stat().st_size > 200]
    if not modules:
        pytest.skip(f"{layer} not implemented yet")

    importers = [p.name for p in modules if "fleet.transforms" in p.read_text()]
    assert importers, f"{layer} exists but no module imports fleet.transforms"
