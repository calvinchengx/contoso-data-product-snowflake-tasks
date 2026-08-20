"""Consume the ERP change stream and land it in the internal stage.

This is the boundary. Everything upstream -- Postgres, Debezium, Redpanda -- is
the world outside the warehouse; everything downstream is inside it. The
consumer is the only thing that touches both, which is exactly where a real
ingestion job sits.

WHY A STREAM AND NOT A TABLE READ. The ERP is the one vendor whose value is its
history. Reading `erp.customer` directly would produce rows -- possibly even a
plausible count -- while testing something else entirely: a snapshot cannot say
that a customer was in Germany before they were in France, and SCD2 over a
snapshot is SCD1 wearing a longer name.

WHAT IS PRESERVED AND WHAT IS NOT. Counts survive real CDC: the same DML
produces the same events. LSNs, commit timestamps and Kafka offsets do not --
they differ every run, and nothing here asserts on them. `effective_date`
travels as DATA, which keeps the fixture's deliberate disagreement between
capture order and business order intact.

LANDED AS CSV, for the reason ingest_reference gives: TYPE = PARQUET copies
report `ok` and load nothing here (snowflake-emulator#20). The change log is a
flat table of scalars, so CSV loses nothing.
"""

from __future__ import annotations

import csv
import json
import os
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from target import T
from sources import ERP_DB, ERP_HOST, ERP_PORT, ERP_TOPIC, ERP_USER, REDPANDA
from stage import WORK, put

TOPIC = ERP_TOPIC

# Debezium's op codes. `r` is a snapshot read: it must not appear, because the
# connector is registered before any DML -- and if it does, that is a finding
# about the ordering, not a row to quietly relabel.
OPS = {"c": "I", "u": "U", "d": "D"}

COLUMNS = [
    "erp_customer_id", "phone", "legal_name", "account_tier", "segment",
    "credit_band", "account_status", "payment_terms_days", "country",
    "effective_date",
]


def watermark(consumer: Consumer, *, ready_timeout: float = 180.0) -> int:
    """The high watermark, having first waited for the topic to exist.

    `make up --wait` WAITS ON CONTAINER HEALTH, and a healthy Kafka Connect is
    not a created topic. The connector registers, then creates the topic, then
    snapshots; asking inside that window raises `_UNKNOWN_PARTITION`, which
    reads like a failure and means "not yet". This step failed three times in
    one afternoon for that reason alone, and each failure looked like a
    different problem because the message names a partition rather than a race.

    Bounded, because a topic that never appears is a real fault -- a connector
    that failed to register, or a name that does not match -- and waiting
    forever would turn it into a hang instead of an error.
    """
    deadline = time.time() + ready_timeout
    while True:
        try:
            _, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=30)
            return high
        except KafkaException as exc:
            code = exc.args[0].code() if exc.args else None
            if code != KafkaError._UNKNOWN_PARTITION:
                raise
            if time.time() >= deadline:
                raise SystemExit(
                    f"the topic {TOPIC} did not appear within {ready_timeout:.0f}s. "
                    f"The connector registers it, so this is a connector fault "
                    f"rather than a slow start -- check contoso-erp-connect."
                ) from exc
            time.sleep(2.0)


def settled(consumer: Consumer, polls: int = 3, gap: float = 5.0) -> int:
    """The high watermark, once it has stopped moving.

    THE ALTERNATIVE IS A SLEEP, and a fixed wait is a flake generator: it passes
    on an idle machine, fails on a loaded one, and -- worse -- passes with a
    PARTIAL stream, landing a shorter file that every count stated as a minimum
    would still accept.
    """
    stable, last = 0, -1
    while stable < polls:
        high = watermark(consumer)
        stable = stable + 1 if high == last and high > 0 else 0
        last = high
        if stable < polls:
            time.sleep(gap)
    return last


def surviving_customers() -> int:
    """What the ERP holds now, asked of the ERP.

    The reconciliation this exists for: the stream's inserts minus its deletes
    must equal the table. A stream that stopped early is stable, well-formed and
    short -- and this is the only check here that notices.
    """
    import psycopg

    # The vendor's own dev credential, declared in its sources.yaml and handed
    # to the containers by the generated fragment. Not a Contoso secret this
    # platform holds: reading a count is a consumer verifying what it consumed.
    password = os.environ.get("ERP_PASSWORD", "contoso-erp-dev")
    dsn = (
        f"host={ERP_HOST} port={ERP_PORT} dbname={ERP_DB} "
        f"user={ERP_USER} password={password}"
    )
    with psycopg.connect(dsn, connect_timeout=30) as conn:
        return conn.execute("SELECT count(*) FROM erp.customer").fetchone()[0]


def main() -> int:
    consumer = Consumer({
        "bootstrap.servers": REDPANDA,
        "group.id": "contoso-erp-ingest-snowflake",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.assign([TopicPartition(TOPIC, 0, 0)])

    high = settled(consumer)
    assert high > 0, (
        f"the change stream {TOPIC!r} is empty — Debezium captured nothing, "
        f"which usually means the connector was registered after the replay"
    )

    rows = []
    while len(rows) < high:
        msg = consumer.poll(30.0)
        assert msg is not None, f"stream stalled at {len(rows):,}/{high:,}"
        assert not msg.error(), msg.error()
        raw = msg.value()
        assert raw is not None, (
            f"tombstone at offset {msg.offset()} — tombstones.on.delete drifted"
        )
        env = json.loads(raw)
        op = env["op"]
        assert op in OPS, (
            f"unexpected Debezium op {op!r} at offset {msg.offset()} — 'r' means "
            f"a snapshot read, so the connector started after the DML"
        )
        # A delete carries its row in `before`, an insert and an update in
        # `after`. REPLICA IDENTITY FULL is what makes the delete's before-image
        # complete; without it an SCD2 build cannot close the version it
        # belonged to, and the past is silently erased.
        image = env["before"] if op == "d" else env["after"]
        assert image, f"{op} at offset {msg.offset()} carried no row image"
        rows.append({"op": OPS[op], "capture_offset": msg.offset(),
                     **{c: image[c] for c in COLUMNS}})
    consumer.close()

    by_op = {o: sum(1 for r in rows if r["op"] == o) for o in ("I", "U", "D")}
    # ALL THREE, because a stream carrying only inserts is a snapshot that
    # arrived over Kafka. The updates are what SCD2 is built from and the
    # deletes are what closes a version.
    assert all(by_op[o] > 0 for o in ("I", "U", "D")), (
        f"the stream carries {by_op} — a change log missing an op class is not "
        f"a change log, it is a snapshot with extra steps"
    )

    # THE RECONCILIATION. Everything above proves the stream is well-formed and
    # that we read all of it; only this proves it is COMPLETE.
    surviving = surviving_customers()
    net = by_op["I"] - by_op["D"]
    if net != surviving:
        longer = net > surviving
        raise SystemExit(
            f"the captured stream implies {net:,} surviving customers "
            f"({by_op['I']:,} inserted − {by_op['D']:,} deleted) but the ERP holds "
            f"{surviving:,}. The stream is "
            + (
                "LONGER than the source: the vendor's history has been replayed "
                "into a topic that still held an earlier run. The seeder truncates "
                "the TABLE; it does not clear the BROKER."
                if longer else
                "SHORT: Debezium did not capture the whole replay."
            )
        )

    dest = WORK / "contoso_erp_changes"
    dest.mkdir(parents=True, exist_ok=True)
    for stale in dest.glob("*.csv"):
        stale.unlink()
    fields = ["op", "capture_offset", *COLUMNS]
    with (dest / "part-0001.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(rows)

    # UPLOADED, not written into place -- see steps/stage.py (G46).
    put(T(), "contoso_erp_changes", sorted(dest.glob("*.csv")))

    print(
        f"Contoso ERP: {len(rows):,} change events consumed from Kafka "
        f"({by_op['I']:,} I / {by_op['U']:,} U / {by_op['D']:,} D), "
        f"reconciled against {surviving:,} surviving rows"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
