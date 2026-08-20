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
from stage import WORK, staged
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


def _load(t, table: str, subdir: str, shape: str) -> int:
    cols = _header(subdir)
    assert len(cols) >= 1, f"{subdir}: the header did not parse: {cols[:3]}"
    if shape == "text" and cols != ["doc"]:
        raise SystemExit(f"{subdir}: expected a single `doc` column, got {cols[:3]}")

    ddl = ", ".join(f'"{c}" {ty}' for c, ty in _csv_types(subdir, cols, shape).items())
    out = sql(t, f"CREATE OR REPLACE TABLE {table} ({ddl})")
    if not out.get("success"):
        raise SystemExit(out)

    # ONE COPY PER PART, because this engine does not read a prefix. Snowflake
    # itself does -- `COPY INTO t FROM @~/dir/` is the ordinary form, and the
    # vendor pages precisely so that a directory of parts is what arrives. The
    # emulator refuses the prefix form BY NAME (snowflake-emulator#41; it used
    # to answer `ok` and load nothing, which is why this loop exists). Naming
    # each part is the form that works, and the loop is what a prefix would have
    # done.
    #
    # THE NAMES COME FROM `LIST`, not from a glob of the client's filesystem.
    # That is the half of G46 that is easy to miss: the copies were always
    # per-file and correct, and the DISCOVERY above them read a directory only
    # the emulator has. `staged()` asks the warehouse, which is the only
    # filesystem that exists on both targets -- and it also means this loop
    # copies what is actually there rather than what ingest believes it wrote.
    parts = staged(t, subdir)
    for part in parts:
        out = sql(
            t,
            f"COPY INTO {table} FROM '@~/{subdir}/{part}' "
            f"FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)",
        )
        if not out.get("success"):
            raise SystemExit(out)

    out = sql(t, f"SELECT count(*) AS n FROM {table}")
    rows = int((out.get("data") or {}).get("rowset", [["0"]])[0][0])

    # COPY INTO REPORTS `ok` WHETHER OR NOT IT LOADED ANYTHING -- measured, and
    # filed as snowflake-emulator#20 for the JSON and PARQUET formats, which
    # load nothing and say ok. So the row count is the only evidence that this
    # step did what it claims, and an empty bronze fails here rather than
    # surfacing as a gold star full of truthful-looking zeros.
    if rows == 0:
        raise SystemExit(
            f"COPY INTO reported success and {table} is empty — "
            f"{len(parts)} part(s) staged under {subdir} and nothing loaded."
        )
    if shape == "text":
        rows = _parse_documents(t, table, subdir)
        cols = _contract_columns(table, subdir)
    print(f"  {table:26} {rows:>8,} rows x {len(cols):>3} col(s), {len(parts)} part(s)")
    return rows


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


def _parse_documents(t, table: str, subdir: str) -> int:
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
    """
    columns = _contract_columns(table, subdir)
    types = _document_types(subdir, columns)
    projected = ", ".join(_project(c, types[c]) for c in columns)
    for stmt in (
        f"CREATE OR REPLACE TABLE {table}_doc AS SELECT PARSE_JSON(doc) AS v FROM {table}",
        f"CREATE OR REPLACE TABLE {table} AS SELECT {projected} FROM {table}_doc",
        f"DROP TABLE {table}_doc",
    ):
        out = sql(t, stmt)
        if not out.get("success"):
            raise SystemExit(f"{table}: parsing the documents failed: {out}")
    out = sql(t, f"SELECT count(*) AS n FROM {table}")
    return int((out.get("data") or {}).get("rowset", [["0"]])[0][0])


def main() -> int:
    t = T()
    total = 0
    for table, subdir, shape in FEEDS:
        total += _load(t, table, subdir, shape)
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
    print(f"bronze: {total:,} rows across {len(FEEDS)} tables, by COPY INTO — contract met")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
