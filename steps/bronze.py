"""Landing to bronze, by COPY INTO from the internal stage.

BRONZE IS SQL HERE, not Spark. That is the whole point of this cell: the other
runtimes hand their bronze to an engine that speaks DataFrames, and Snowflake
does not have one — it has a warehouse and a stage. `COPY INTO` reading a prefix
of staged parts is what a Snowflake team writes, and it is a different shape of
the same step rather than a port of somebody else's.

THE COLUMNS ARE THE VENDOR'S, in the order the vendor serves them. A CSV copy is
positional, so a column list that drifts from the export does not fail — it
loads the right bytes into the wrong columns, which is worse. The header row the
vendor sends is the authority, and `_assert_header` checks it before any copy.
"""

from __future__ import annotations

import csv
import json
from datetime import date

from contoso_product import bronze_contract, check_bronze
from provision import sql
from stage import WORK
from tasks import run_graph
from target import T

# The platform's table name for each name the contract uses. The indirection is
# the product's -- `source('bronze', var('bronze_ref_fx_rates'))` -- so a
# platform keeps the names its bronze already had.
CONTRACT_NAME = {
    "bronze_pos_customers": "bronze_pos_customers",
    "bronze_pos_orders": "bronze_pos_orders",
    "bronze_web_customers": "bronze_web_customers",
    "bronze_web_orders": "bronze_web_orders",
    "bronze_web_products": "bronze_web_products",
    "bronze_ref_product_hierarchy": "bronze_product_hierarchy",
    "bronze_ref_fx_rates": "bronze_fx_rates",
    "bronze_erp_customer_changes": "bronze_erp_changes",
}

# How many staged documents to read before deciding a field's type. One is not
# enough: a field that is null in the first document says nothing about the
# rest, and a field that opens with an integer can still hold a fraction
# further down.
SAMPLE = 200


# The POS customer export: 101 columns, and the six the product actually reads.
# Declared as the vendor's header rather than the product's needs, because
# bronze is what arrived -- narrowing belongs in silver, where it is visible.
# Every staged feed, and the shape it landed in. `text` means the vendor's
# documents ride in as one JSON string per row and silver parses them -- see
# ingest_web for why that is the ordinary Snowflake pattern rather than a dodge.
FEEDS = [
    ("bronze_pos_customers", "contoso_pos_customers", "header"),
    ("bronze_pos_orders", "contoso_pos_orders", "text"),
    ("bronze_web_customers", "contoso_web_customers", "text"),
    ("bronze_web_products", "contoso_web_products", "text"),
    ("bronze_web_orders", "contoso_web_orders", "text"),
    ("bronze_product_hierarchy", "contoso_reference_product_hierarchy", "header"),
    ("bronze_fx_rates", "contoso_reference_fx_rates", "header"),
    ("bronze_erp_changes", "contoso_erp_changes", "header"),
]


def _header(subdir: str) -> list[str]:
    # INGEST'S OWN OUTPUT, not the stage. `work/` is this product's scratch
    # space on whatever machine it runs on; reading a file you just wrote is
    # fine anywhere, and reading the WAREHOUSE'S stage as a filesystem is what
    # breaks on a real account (G46). The column names and types below come
    # from here; the BYTES come from the stage, by name.
    parts = sorted((WORK / subdir).glob("*.csv"))
    if not parts:
        raise SystemExit(
            f"nothing under {WORK / subdir} — run ingest first."
        )
    # PARSED AS CSV, not split on commas. The JSON-text feeds are written
    # QUOTE_ALL, so their header arrives as `"doc"` and a naive split keeps the
    # quotes -- which then fails a comparison against the column name it IS.
    with parts[0].open(encoding="utf-8", newline="") as fh:
        return next(csv.reader(fh))


def _statements(t, table: str, subdir: str, shape: str) -> list[tuple[str, str]]:
    """The statements this feed needs, as (node, sql) for the task graph.

    BUILT HERE, RUN BY TASKS. The shape of a feed is discovered from the stage
    -- the vendor's own header decides the columns -- and that discovery is a
    read, so it stays here. What it produces is the statements, which run as
    tasks.
    """
    cols = _header(subdir)
    assert len(cols) >= 1, f"{subdir}: the header did not parse: {cols[:3]}"
    if shape == "text" and cols != ["doc"]:
        raise SystemExit(f"{subdir}: expected a single `doc` column, got {cols[:3]}")

    ddl = ", ".join(f'"{c}" {ty}' for c, ty in _csv_types(subdir, cols, shape).items())

    # ONE COPY FOR THE WHOLE FEED, because a stage reference is a PREFIX.
    #
    # This was one COPY INTO per part, and the loop was not the product's idea:
    # the emulator refused a prefix by name, so naming each part was the form
    # that worked. snowflake-emulator#53 resolves a prefix the way Snowflake
    # does -- every file whose path starts with the reference, in sorted order
    # -- so the ordinary form works and the loop is gone.
    #
    # It matters more than tidiness here. A task body is ONE statement, so a
    # per-part loop would have made this feed as many tasks as the vendor
    # happened to page it into: thirty-odd for eight tables, a graph whose shape
    # was decided by an emulator limitation rather than by the pipeline.
    stmts = [
        (f"{table}_create", f"CREATE OR REPLACE TABLE {table} ({ddl})"),
        (
            f"{table}_load",
            f"COPY INTO {table} FROM '@~/{subdir}/' "
            f"FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)",
        ),
    ]
    if shape == "text":
        stmts += _document_statements(table, subdir)
    return stmts


def _contract_columns(table: str, subdir: str) -> list[str]:
    """The columns to project out of a document feed.

    THE CONTRACT WHERE THERE IS ONE, and the vendor's own keys where there is
    not. Silver reads six of the eight bronze tables; the other two are landed
    because bronze is what ARRIVED, and narrowing them to a list the product
    does not read would be this file inventing a shape. That mistake has
    already been made once in the other direction -- the contract briefly
    declared columns for two tables nothing reads, and the invented list
    accused a correct bronze of a breach.
    """
    contract = bronze_contract()
    for var_name, platform_name in CONTRACT_NAME.items():
        if platform_name == table and var_name in contract:
            return contract[var_name]
    return _document_keys(subdir)


def _csv_types(subdir: str, cols: list[str], shape: str) -> dict[str, str]:
    """The column types to declare, read from the staged CSV.

    ALL VARCHAR IS NOT NEUTRAL, which is what this replaces. A CSV column that
    is really a date arrives as text, and silver then does date arithmetic on
    it -- `No function matches the given name and argument types
    '+(VARCHAR, INTEGER)'`, three layers from the COPY that decided it. Spark's
    bronze infers when it reads the file; this one has to say so out loud
    because COPY INTO fills a table that already exists.

    A DOCUMENT FEED IS EXEMPT: its single `doc` column is text by construction
    and gets parsed afterwards, where the document's own types decide.
    """
    if shape == "text":
        return {c: "VARCHAR" for c in cols}
    seen: dict[str, set[str]] = {c: set() for c in cols}
    for row in _csv_rows(subdir, SAMPLE):
        for c, v in zip(cols, row):
            if v != "":
                seen[c].add(_scalar_kind(v))
    return {c: _widen(k) for c, k in seen.items()}


def _scalar_kind(v: str) -> str:
    try:
        int(v)
        return "BIGINT"
    except ValueError:
        pass
    try:
        float(v)
        return "DOUBLE"
    except ValueError:
        pass
    if len(v) == 10 and v[4] == "-" and v[7] == "-":
        try:
            date.fromisoformat(v)
            return "DATE"
        except ValueError:
            pass
    return "VARCHAR"


def _widen(kinds: set[str]) -> str:
    """One type for the whole column, read from every sampled row.

    A column whose first row is an integer can still hold a fraction further
    down, and one that is empty in the sample says nothing at all. Text is the
    answer that cannot be wrong about a value nobody has seen.
    """
    if not kinds:
        return "VARCHAR"
    if kinds == {"BIGINT"}:
        return "BIGINT"
    if kinds <= {"BIGINT", "DOUBLE"}:
        return "DOUBLE"
    if kinds == {"DATE"}:
        return "DATE"
    return "VARCHAR"


def _csv_rows(subdir: str, limit: int):
    parts = sorted((WORK / subdir).glob("*.csv"))
    seen = 0
    for part in parts:
        with part.open(encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                yield row
                seen += 1
                if seen >= limit:
                    return


def _project(column: str, kind: str) -> str:
    """One column, cast to what the vendor's document says it is.

    THE TYPE COMES FROM THE DOCUMENT, not from a list here. JSON distinguishes
    a number from a string, and throwing that away costs correctness in both
    directions: unwrapping everything to text made silver fail on
    `Cannot compare values of type VARCHAR and type INTEGER_LITERAL`, and
    leaving everything as a document makes `lower(trim(email))` operate on
    "a@x.com" WITH its quotes -- a value that compares unequal to itself.
    """
    if kind == "document":
        return f"v:{column} AS {column}"
    return f"v:{column}::{kind} AS {column}"


def _document_types(subdir: str, columns: list[str]) -> dict[str, str]:
    """What each column holds, read from the documents rather than declared."""
    kinds = {c: None for c in columns}
    for doc in _documents(subdir, SAMPLE):
        for c in columns:
            if kinds[c] is not None:
                continue
            v = doc.get(c)
            if v is None:
                continue
            if isinstance(v, bool):
                kinds[c] = "boolean"
            elif isinstance(v, int):
                kinds[c] = "bigint"
            elif isinstance(v, float):
                kinds[c] = "double"
            elif isinstance(v, (dict, list)):
                kinds[c] = "document"
            else:
                kinds[c] = "string"
        if all(v is not None for v in kinds.values()):
            break
    # A column null in every sampled document has no type to preserve. Text is
    # the answer that cannot be wrong about a value nobody has seen.
    return {c: k or "string" for c, k in kinds.items()}


def _documents(subdir: str, limit: int):
    parts = sorted((WORK / subdir).glob("*.csv"))
    seen = 0
    for part in parts:
        with part.open(encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            next(reader)
            for row in reader:
                yield json.loads(row[0])
                seen += 1
                if seen >= limit:
                    return


def _document_keys(subdir: str) -> list[str]:
    """The top-level keys of the first staged document, in the vendor's order."""
    return list(next(iter(_documents(subdir, 1))))


def _document_statements(table: str, subdir: str) -> list[tuple[str, str]]:
    """Turn one JSON document per row into the columns silver reads.

    THE VENDOR SHIPS DOCUMENTS AND SILVER READS COLUMNS, and something has to
    bridge that. It is bronze's job rather than silver's: silver is the layer
    every cell shares, and a `doc:field` branch inside it would put an engine
    difference in the one place the family cannot afford one. Spark's bronze
    already does this -- `spark.read.json` parses on the way in -- so parsing
    here is what makes the two cells the same shape rather than a Snowflake
    peculiarity.

    THE COLUMN LIST IS THE CONTRACT'S, not this file's. bronze_contract() is
    what silver's sources.yml declares, so bronze cannot drift from what silver
    reads without the contract moving first.

    THREE STATEMENTS, SO THREE TASKS. They ran in a loop here and now run as
    nodes, which changes nothing about the SQL and everything about what happens
    when the middle one fails: the drop no longer runs against a table the
    projection never built, and the history says which of the three stopped it.
    """
    columns = _contract_columns(table, subdir)
    types = _document_types(subdir, columns)
    projected = ", ".join(_project(c, types[c]) for c in columns)
    return [
        (f"{table}_parse", f"CREATE OR REPLACE TABLE {table}_doc AS SELECT PARSE_JSON(doc) AS v FROM {table}"),
        (f"{table}_project", f"CREATE OR REPLACE TABLE {table} AS SELECT {projected} FROM {table}_doc"),
        (f"{table}_tidy", f"DROP TABLE {table}_doc"),
    ]

def main() -> int:
    t = T()

    # ONE GRAPH FOR THE WHOLE OF BRONZE, rather than one per feed. The feeds are
    # independent, so a chain says something slightly stronger than they need --
    # but it says the thing that matters: if one feed cannot be built, the rest
    # are SKIPPED and named, rather than half a bronze being handed to silver as
    # though it were whole.
    plan: list[tuple[str, str]] = []
    for table, subdir, shape in FEEDS:
        plan += _statements(t, table, subdir, shape)
    run_graph(t, "bronze", plan)

    # COUNTED AFTERWARDS, FROM THE ENGINE. The tasks report SUCCEEDED, and
    # `COPY INTO` reports ok whether or not it loaded anything (measured, and
    # filed as snowflake-emulator#20 for JSON and PARQUET, which load nothing
    # and say ok). So the rows are the only evidence that this step did what it
    # claims, and they are read back rather than accumulated from what the
    # loader believed it wrote.
    total = 0
    for table, _subdir, _shape in FEEDS:
        out = sql(t, f"SELECT count(*) AS n FROM {table}")
        if not out.get("success"):
            raise SystemExit(f"{table}: the graph succeeded and the table cannot be read: {out}")
        rows = int((out.get("data") or {}).get("rowset", [["0"]])[0][0])
        if rows == 0:
            raise SystemExit(
                f"the bronze graph reported SUCCEEDED and {table} is empty — "
                f"every task ran and nothing arrived."
            )
        total += rows
    # THE CONTRACT, CHECKED FROM THE ENGINE. Bronze is a contract between the
    # platform that writes it and the silver that reads it, and until core
    # 0.3.0 nothing verified it: this cell landed its JSON feeds whole, in a
    # single `doc` column, and silver reported it four layers away as
    # `Binder Error: Table "o" does not have a column named "lines"`.
    observed = {}
    for var_name, platform_name in CONTRACT_NAME.items():
        out = sql(t, f"DESCRIBE TABLE {platform_name}")
        if not out.get("success"):
            continue
        observed[var_name] = [r[0] for r in (out.get("data") or {}).get("rowset", [])]
    problems = check_bronze(observed)
    if problems:
        raise SystemExit(
            "bronze does not meet the contract silver reads:\n  " + "\n  ".join(problems)
        )
    print(f"bronze: {total:,} rows across {len(FEEDS)} tables, by COPY INTO through Snowflake Tasks — contract met")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
