"""Checks on the report and its figures.

- every figure the report includes exists, and every diagram has a draw.io source;
- screenshots are real PNGs of a reasonable size (the capture script produces them
  from the running stack);
- the report text follows the agreed rules: no hash character, no em or en dash, no
  mention of the other coursework project, no unfilled measurement.
"""

from __future__ import annotations

import re
import shutil
import struct
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = REPO_ROOT / "docs" / "report"
FIGURES_DIR = REPORT_DIR / "figures"
DIAGRAMS_DIR = REPO_ROOT / "docs" / "diagrams"
MAIN_TEX = REPORT_DIR / "main.tex"
MEASUREMENTS_TEX = REPORT_DIR / "measurements.tex"


def _tex() -> str:
    """The report source without LaTeX comments."""
    text = MAIN_TEX.read_text() + MEASUREMENTS_TEX.read_text()
    return "\n".join(re.split(r"(?<!\\)%", line, maxsplit=1)[0] for line in text.splitlines())


def _referenced_figures() -> list[str]:
    tex = MAIN_TEX.read_text()
    shots = [f"{name}.png" for name in re.findall(r"\\shot\{([^}]+)\}", tex)]
    graphics = [Path(p).name for p in re.findall(r"\\includegraphics\[[^]]*\]\{([^}]+)\}", tex)]
    # Skip the \shot macro's own definition, whose path is the parameter #1.
    return [g for g in shots + graphics if g != "Logo.jpeg" and "#" not in g]


@pytest.mark.parametrize("name", _referenced_figures())
def test_every_referenced_figure_exists(name: str) -> None:
    path = FIGURES_DIR / name
    assert path.exists(), f"main.tex includes {name}, which is not in docs/report/figures"
    if name.endswith(".png"):
        head = path.read_bytes()[:24]
        assert head[:8] == b"\x89PNG\r\n\x1a\n", f"{name} is not a PNG"
        width, height = struct.unpack(">II", head[16:24])  # the IHDR chunk
        assert width >= 600 and height >= 150, f"{name} looks too small"


def test_every_diagram_has_a_drawio_source() -> None:
    pdfs = list(FIGURES_DIR.glob("D*.pdf"))
    assert pdfs, "no diagrams in docs/report/figures"
    for pdf in pdfs:
        assert (DIAGRAMS_DIR / f"{pdf.stem}.drawio").exists(), f"{pdf.name} has no .drawio"


def test_no_tikz_diagram_sources_remain() -> None:
    assert not list(DIAGRAMS_DIR.glob("*.tex")), "diagrams are drawn in draw.io now"


def test_report_uses_no_hash_character() -> None:
    body = re.sub(r"#[1-9]", "", _tex())  # LaTeX macro parameters, not text
    assert "#" not in body


def test_report_uses_no_em_or_en_dash() -> None:
    body = _tex()
    for dash in ("---", "--", "\u2014", "\u2013", "\\textemdash", "\\textendash"):
        assert dash not in body, f"the report contains {dash!r}"


def test_report_does_not_mention_the_other_project() -> None:
    text = _tex().lower()
    for word in ("hospital", "vitals", "ward", "patient", "sibling", "news2"):
        assert not re.search(rf"\b{word}\b", text), f"the report mentions {word!r}"


def test_no_measurement_is_left_unfilled() -> None:
    assert "TBD" not in MEASUREMENTS_TEX.read_text()


def test_every_table_is_fully_bordered() -> None:
    for spec in re.findall(r"\\begin\{tabularx?\}(?:\{[^}]*\})?\{([^}]*)\}", _tex()):
        if spec == "ll":  # the cover page's name list is not a table
            continue
        assert spec.startswith("|") and spec.endswith("|"), f"table {{{spec}}} has open sides"


@pytest.mark.skipif(shutil.which("pdftotext") is None, reason="needs poppler")
def test_built_pdf_has_no_hash_dash_or_placeholder() -> None:
    pdf = REPORT_DIR / "main.pdf"
    if not pdf.exists():
        pytest.skip("main.pdf not built")
    text = subprocess.run(
        ["pdftotext", str(pdf), "-"], capture_output=True, text=True, check=True
    ).stdout
    assert "#" not in text
    assert "\u2014" not in text and "\u2013" not in text, "an em or en dash is in the PDF"
    assert "??" not in text, "an unresolved reference (??) is in the PDF"
