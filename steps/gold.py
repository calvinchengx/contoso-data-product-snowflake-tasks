"""dbt-snowflake over the product gold project. Adapter only; SQL is the product's."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from urllib.request import Request, urlopen

from contoso_product import gold_dir
from contracts import check, read_run_results, summarise
from target import DATABASE, SCHEMA_SILVER, T, WAREHOUSE
from tasks import env_vars_clause, fetch_dbt_output, run_graph, stage_dbt_project


def main() -> int:
    t = T()
    product = gold_dir()
    work = Path("gold")
    for name in ("models", "macros", "tests"):
        dest = work / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(product / name, dest)

    # NO CONNECTION SETTINGS, AND NO SUBPROCESS. dbt runs inside the account
    # now: the project is staged, named by CREATE DBT PROJECT, and every
    # invocation after that is a statement.
    #
    # DBT_-PREFIXED, and that is not this platform's preference. dbt Projects on
    # Snowflake namespaces what a project may read -- "Every key in env: and in
    # any override must be prefixed with DBT_ ... Every key must be UPPERCASE",
    # enforced on every run -- so `CONTOSO_SILVER_DATABASE` could not be
    # supplied here by any route. That is what moved the names in core
    # (contoso-data-product#34, v0.6.0), and it also retired the nested
    # `env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID'))` default,
    # whose eager evaluation made a Fabric-only variable mandatory on every
    # engine and stood in the plan for months as a "dialect gap" while gold had
    # in fact never run here.
    stage_dbt_project(t, "gold", work, dbt_vars={})
    env_clause = env_vars_clause(
        {"DBT_SILVER_DATABASE": DATABASE, "DBT_SILVER_SCHEMA": SCHEMA_SILVER}
    )
    run_graph(
        t,
        "gold",
        [
            ("run", f"EXECUTE DBT PROJECT gold ARGS='run'{env_clause}"),
            ("test", f"EXECUTE DBT PROJECT gold ARGS='test'{env_clause}"),
        ],
    )
    # THE DIALECT GAP IS GONE, not silenced. gold.py carried a `dialect_gap`
    # branch that caught a dbt failure and published the snapshot anyway with
    # the reason attached. The reason it actually caught was the missing
    # LAKEHOUSE_ID above -- a parse failure recorded for months as an engine
    # incompatibility. There is nothing left for it to catch that the task graph
    # does not fail on and name, so it is removed rather than kept as a place
    # for the next wrong explanation to live.

    # THE CONTRACTS, ACTUALLY EVALUATED, read from dbt's own record.
    #
    # `dbt test` ran as its own task, so it has its own verdict: the graph
    # stopped if it failed. What the graph cannot say is WHICH contracts were
    # evaluated, and that is the difference between publishing guarantees and
    # publishing guarantees something checked -- the distinction G29 and G41
    # exist for.
    #
    # run_results.json says it, and it comes back from the STAGE rather than
    # from a local target/ directory, because dbt no longer runs on this host.
    # `read_run_results` asserts `args.which == "test"` on it exactly as before,
    # which still matters: `dbt run` and `dbt test` write the same filename, and
    # the archive holds whichever ran last.
    fetch_dbt_output(t, "gold", work)
    results = read_run_results(work)
    tested = summarise(results)
    # The glob is the EXPECTATION now, not the answer: these are the contracts
    # core ships, and `check` refuses unless dbt evaluated and passed every one.
    contracts = check(
        results,
        {p.stem for p in (product / "tests").glob("*.sql")},
        "gold",
    )

    snapshot = {
        "revenue_usd": "0",
        "cancelled_revenue_usd": "0",
        "sale_lines": "0",
        "contracts": contracts,
        "runtime": "snowflake",
        "catalog": DATABASE,
        "engine": "duckdb",
    }
    body = json.dumps(
        {
            "statement": "SELECT coalesce(sum(revenue_usd),0), coalesce(sum(cancelled_revenue_usd),0), coalesce(sum(sale_lines),0) FROM fct_revenue_summary",
            "warehouse": WAREHOUSE,
        }
    ).encode()
    req = Request(
        f"{t.host}/api/v2/statements",
        data=body,
        headers={"Authorization": f"Bearer {t.password}", "Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req) as resp:
        out = json.loads(resp.read())
    rows = (out.get("data") or {}).get("rowset") or []
    if rows:
        snapshot["revenue_usd"] = str(rows[0][0])
        snapshot["cancelled_revenue_usd"] = str(rows[0][1])
        snapshot["sale_lines"] = str(rows[0][2])
    if tested:
        snapshot["data_tests"] = tested
    Path("product_snapshot.json").write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(f"gold contracts: {tested}")
    print(f"gold snapshot {snapshot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
