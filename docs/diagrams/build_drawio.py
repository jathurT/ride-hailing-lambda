"""Write the report's diagrams as draw.io files, then export them to PDF.

    .venv/bin/python docs/diagrams/build_drawio.py      # writes *.drawio
    make -C docs/diagrams                               # exports *.pdf with the drawio CLI

The .drawio files are ordinary draw.io documents and can be opened and edited in
draw.io directly. This script keeps the six diagrams in one style: every diagram flows
top to bottom (it stays legible at page width), all text is 13 pt, labels are short,
and one colour key is used: blue = streaming, orange = storage and serving,
green = orchestration, grey = observability, white = outside the platform.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from xml.sax.saxutils import escape

HERE = Path(__file__).resolve().parent

FONT = "fontFamily=Helvetica;fontSize=13;"
BASE = "whiteSpace=wrap;html=1;rounded=1;arcSize=8;" + FONT
STYLE = {
    "stream": BASE + "fillColor=#dae8fc;strokeColor=#6c8ebf;",
    "store": BASE + "fillColor=#ffe6cc;strokeColor=#d79b00;",
    "orch": BASE + "fillColor=#d5e8d4;strokeColor=#82b366;",
    "obs": BASE + "fillColor=#f5f5f5;strokeColor=#666666;",
    "ext": BASE + "fillColor=#ffffff;strokeColor=#333333;",
    "group": "whiteSpace=wrap;html=1;rounded=1;arcSize=4;verticalAlign=top;align=left;"
    "spacingLeft=8;" + FONT + "fontStyle=1;fillColor=#eef4fc;strokeColor=#6c8ebf;",
    "topic": "whiteSpace=wrap;html=1;rounded=0;" + FONT + "fillColor=#ffffff;strokeColor=#6c8ebf;",
    "label": "text;html=1;align=left;verticalAlign=middle;" + FONT + "fontStyle=1;",
    "note": "text;html=1;align=center;verticalAlign=middle;whiteSpace=wrap;" + FONT,
    "decision": "rhombus;whiteSpace=wrap;html=1;" + FONT + "fillColor=#fff2cc;strokeColor=#d6b656;",
}
EDGE = (
    "edgeStyle=orthogonalEdgeStyle;rounded=1;html=1;endArrow=block;endFill=1;"
    + FONT
    + "labelBackgroundColor=#ffffff;strokeColor=#333333;jumpStyle=arc;jumpSize=8;"
)


@dataclass
class Diagram:
    name: str
    cells: list[str] = field(default_factory=list)
    _n: int = 0

    def _id(self) -> str:
        self._n += 1
        return f"c{self._n}"

    def box(
        self,
        label: str,
        x: float,
        y: float,
        w: float,
        h: float,
        kind: str = "ext",
        extra: str = "",
    ) -> str:
        cid = self._id()
        self.cells.append(
            f'<mxCell id="{cid}" value="{escape(label, {chr(34): "&quot;"})}" '
            f'style="{STYLE[kind]}{extra}" vertex="1" parent="1">'
            f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>'
        )
        return cid

    def edge(
        self,
        src: str,
        dst: str,
        label: str = "",
        extra: str = "",
        exit: tuple[float, float] | None = None,
        entry: tuple[float, float] | None = None,
    ) -> str:
        cid = self._id()
        anchors = ""
        if exit:
            anchors += f"exitX={exit[0]};exitY={exit[1]};exitDx=0;exitDy=0;"
        if entry:
            anchors += f"entryX={entry[0]};entryY={entry[1]};entryDx=0;entryDy=0;"
        self.cells.append(
            f'<mxCell id="{cid}" value="{escape(label, {chr(34): "&quot;"})}" '
            f'style="{EDGE}{anchors}{extra}" edge="1" parent="1" source="{src}" '
            f'target="{dst}"><mxGeometry relative="1" as="geometry"/></mxCell>'
        )
        return cid

    def down(self, src: str, dst: str, label: str = "", extra: str = "") -> str:
        """An arrow from the bottom of src to the top of dst."""
        return self.edge(src, dst, label, extra, exit=(0.5, 1), entry=(0.5, 0))

    def write(self) -> Path:
        xml = (
            f'<mxfile host="build_drawio.py"><diagram id="{self.name}" name="{self.name}">'
            '<mxGraphModel grid="0" page="0" math="0" shadow="0"><root>'
            '<mxCell id="0"/><mxCell id="1" parent="0"/>'
            f"{''.join(self.cells)}</root></mxGraphModel></diagram></mxfile>\n"
        )
        path = HERE / f"{self.name}.drawio"
        path.write_text(xml)
        return path


def d1_context() -> Diagram:
    d = Diagram("D1-context")
    core = d.box(
        "<b>Fleet operations platform</b><br>Lambda: a speed layer and a batch layer",
        120,
        150,
        480,
        70,
        "stream",
    )
    fleet = d.box("<b>Vehicle fleet</b><br>150 vehicles", 60, 0, 240, 50)
    partners = d.box("<b>Fuel and garage partners</b><br>one file per day", 420, 0, 240, 50)
    d.edge(fleet, core, "telemetry", exit=(0.5, 1), entry=(0.25, 0))
    d.edge(partners, core, "expenses", exit=(0.5, 1), entry=(0.75, 0))
    users = [
        ("<b>Dispatch</b>", "live use,<br>idle alerts"),
        ("<b>Finance</b>", "daily<br>profit and loss"),
        ("<b>Operations</b>", "pipeline<br>alerts"),
    ]
    for i, (text, label) in enumerate(users):
        b = d.box(text, i * 260, 330, 200, 45)
        d.edge(core, b, label, exit=(0.15 + i * 0.35, 1), entry=(0.5, 0))
    return d


def d2_architecture() -> Diagram:
    d = Diagram("D2-layered-architecture")
    for y, text in [
        (20, "Sources"),
        (150, "Log"),
        (262, "Speed layer"),
        (380, "Storage"),
        (505, "Batch layer"),
        (607, "Batch view"),
        (720, "Serving"),
    ]:
        d.box(text, 0, y, 115, 40, "label")

    prod = d.box("<b>Telemetry producer</b><br>150 vehicles, 240 events/s", 130, 10, 340, 55)
    drop = d.box("<b>Expense dropper</b><br>one CSV per sim day", 510, 10, 340, 55)

    d.box("Apache Kafka :9092", 120, 110, 360, 100, "group")
    t_tel = d.box("<b>fleet.telemetry.v1</b><br>6 partitions", 130, 148, 165, 50, "topic")
    t_reg = d.box("<b>fleet.vehicle.registry</b><br>compacted", 305, 148, 165, 50, "topic")

    speed = d.box(
        "<b>Spark Structured Streaming</b><br>6 queries, 10 s trigger<br>validate, enrich, window",
        130,
        245,
        340,
        75,
        "stream",
    )
    redis = d.box("<b>Redis</b> :6389<br>speed view, expires", 130, 370, 220, 60, "store")
    lake = d.box(
        "<b>MinIO lake</b> :9000<br>raw events as Parquet, expense CSVs",
        400,
        370,
        450,
        60,
        "store",
    )
    batch = d.box(
        "<b>Spark batch</b>, daily<br>zone hourly, vehicle profit and loss",
        510,
        490,
        340,
        70,
        "orch",
    )
    pg = d.box(
        "<b>PostgreSQL</b> :5442<br>star schema, high-water mark", 510, 600, 340, 55, "store"
    )
    api = d.box("<b>FastAPI</b> :8000<br>merges speed and batch views", 130, 710, 340, 60, "store")
    graf = d.box("<b>Grafana</b> :3000<br>three dashboards", 510, 710, 340, 60, "obs")

    reg = d.box("<b>Schema Registry</b><br>:8081, Avro", 890, 148, 200, 50, "stream")
    out = d.box(
        "<b>Kafka outputs</b><br>fleet.telemetry.dlq<br>fleet.alerts.v1",
        890,
        250,
        200,
        70,
        "stream",
    )
    air = d.box(
        "<b>Airflow</b> :8082<br>runs the batch<br>once a sim day", 890, 490, 200, 70, "orch"
    )
    prom = d.box("<b>Prometheus</b> :9090<br>Alertmanager :9093", 890, 710, 200, 60, "obs")

    d.edge(prod, t_tel, exit=(0.24, 1), entry=(0.5, 0))
    d.edge(t_tel, speed, "", exit=(0.5, 1), entry=(0.24, 0))
    d.edge(t_reg, speed, "", exit=(0.5, 1), entry=(0.76, 0))
    d.edge(speed, redis, "", exit=(0.32, 1), entry=(0.5, 0))
    d.edge(speed, lake, "raw", exit=(0.85, 1), entry=(0.13, 0))
    d.edge(drop, lake, "daily CSV", exit=(0.5, 1), entry=(0.62, 0))
    d.edge(lake, batch, "", exit=(0.8, 1), entry=(0.735, 0))
    d.down(batch, pg)
    d.edge(redis, api, "", exit=(0.5, 1), entry=(0.32, 0))
    d.edge(pg, api, "", exit=(0.2, 1), entry=(0.8, 0))
    d.edge(api, graf, exit=(1, 0.5), entry=(0, 0.5))
    d.edge(speed, out, exit=(1, 0.5), entry=(0, 0.5))
    d.edge(air, batch, "", "dashed=1;", exit=(0, 0.5), entry=(1, 0.5))
    d.edge(reg, t_reg, "", "dashed=1;", exit=(0, 0.5), entry=(1, 0.5))
    d.edge(prom, graf, exit=(0, 0.5), entry=(1, 0.5))

    for i, (kind, text) in enumerate(
        [
            ("stream", "Streaming"),
            ("store", "Storage, serving"),
            ("orch", "Batch, orchestration"),
            ("obs", "Observability"),
            ("ext", "Outside"),
        ]
    ):
        d.box(text, 130 + i * 190, 805, 170, 30, kind)
    return d


def d3_event() -> Diagram:
    d = Diagram("D3-event-sequence")
    ping = d.box("<b>Vehicle</b> sends a ping<br>Avro, key = vehicle", 175, 0, 350, 50)
    kafka = d.box("<b>Kafka</b><br>fleet.telemetry.v1", 175, 100, 350, 50, "stream")
    spark = d.box(
        "<b>Spark micro-batch</b>, every 10 s<br>validate, enrich, window",
        175,
        200,
        350,
        60,
        "stream",
    )
    redis = d.box("<b>Redis</b><br>live zone figures, approximate", 0, 320, 300, 50, "store")
    lake = d.box("<b>MinIO</b><br>the raw event, unchanged", 400, 320, 300, 50, "store")
    batch = d.box(
        "<b>Next sim day</b>: Airflow runs<br>Spark batch, exact figures", 400, 430, 300, 50, "orch"
    )
    pg = d.box("<b>PostgreSQL</b><br>batch view", 400, 540, 300, 50, "store")
    api = d.box(
        "<b>FastAPI merge</b><br>batch up to the high-water mark,<br>speed after it",
        175,
        650,
        350,
        70,
        "store",
    )
    d.down(ping, kafka)
    d.down(kafka, spark)
    d.edge(spark, redis, "speed view", exit=(0.25, 1), entry=(0.5, 0))
    d.edge(spark, lake, "master data", exit=(0.75, 1), entry=(0.5, 0))
    d.down(lake, batch)
    d.down(batch, pg)
    d.edge(redis, api, "", exit=(0.5, 1), entry=(0.2, 0))
    d.edge(pg, api, "", exit=(0.5, 1), entry=(0.8, 0))
    return d


def d4_merge() -> Diagram:
    d = Diagram("D4-merge-boundary")
    d.box("<b>(a) Healthy</b><br>high-water mark: d minus 1", 0, 0, 300, 40, "note")
    d.box("<b>(b) Batch three days behind</b><br>high-water mark: d minus 3", 400, 0, 300, 40, "note")
    days = ["d minus 4", "d minus 3", "d minus 2", "d minus 1", "d (today)"]
    healthy = ["batch", "batch", "batch", "batch", "speed"]
    behind = ["batch", "batch", "not covered", "not covered", "speed"]
    kinds = {"batch": "orch", "speed": "stream", "not covered": "ext"}
    for i, day in enumerate(days):
        y = 55 + i * 60
        for x, owner in ((0, healthy[i]), (400, behind[i])):
            extra = "dashed=1;" if owner == "not covered" else ""
            d.box(f"<b>{day}</b>: {owner}", x, y, 300, 45, kinds[owner], extra)
    d.box(
        "Every date has exactly one owner, so nothing is counted twice.<br>"
        "If the batch falls behind, the gap is reported, not hidden.",
        0,
        365,
        700,
        50,
        "note",
    )
    return d


def d5_storage() -> Diagram:
    d = Diagram("D5-storage")
    boxes = [
        (
            "<b>Redis</b> (speed view, keys expire)<br>fleet:zone:{zone}<br>"
            "fleet:vehicle:{id}:state<br>fleet:ts:zone:{zone}:earnings<br>fleet:alerts:idle",
            "store",
        ),
        (
            "<b>MinIO lake</b> (master dataset, never changed)<br>raw/telemetry/sim_date=.../*.parquet"
            "<br>landing/expenses/expenses_DATE.csv<br>quarantine/, restatements/",
            "store",
        ),
        (
            "<b>PostgreSQL mart</b> (batch view, star schema)<br>dimensions: vehicle, zone, date,"
            " driver<br>facts: vehicle daily profit and loss, zone hourly<br>control: high-water"
            " mark, data quality, reconciliation",
            "store",
        ),
    ]
    prev = None
    for i, (text, kind) in enumerate(boxes):
        b = d.box(text, 0, i * 150, 560, 110, kind, "align=left;spacingLeft=12;")
        if prev:
            d.down(prev, b)
        prev = b
    return d


def d6_dag() -> Diagram:
    d = Diagram("D6-daily-batch")
    start = d.box("<b>Resolve the sim date</b>", 100, 0, 300, 40, "orch")
    snap = d.box("<b>Snapshot the speed view</b>", 100, 80, 300, 40, "orch")
    check = d.box("Expense file<br>valid?", 150, 160, 200, 100, "decision")
    quar = d.box("Quarantine the file,<br>report failure", 440, 185, 220, 50)
    zone = d.box("<b>Spark: zone hourly</b>", 100, 300, 300, 40, "orch")
    pnl = d.box("<b>Spark: vehicle profit and loss</b>", 100, 380, 300, 40, "orch")
    wm = d.box("<b>Check the high-water mark moved</b>", 100, 460, 300, 40, "orch")
    delta = d.box("<b>Speed vs batch difference</b>", 100, 540, 300, 40, "orch")
    d.down(start, snap)
    d.down(snap, check)
    d.edge(check, quar, "corrupt", exit=(1, 0.5), entry=(0, 0.5))
    d.edge(check, zone, "valid or missing", exit=(0.5, 1), entry=(0.5, 0))
    d.down(zone, pnl)
    d.down(pnl, wm)
    d.down(wm, delta)
    return d


if __name__ == "__main__":
    for build in (d1_context, d2_architecture, d3_event, d4_merge, d5_storage, d6_dag):
        print("wrote", build().write().name)
