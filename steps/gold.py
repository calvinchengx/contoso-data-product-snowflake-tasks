"""dbt-snowflake over the product gold project. Adapter only; SQL is the product's."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.request import Request, urlopen

from contoso_product import gold_dir
from contracts import ContractError, check, read_run_results, summarise
from target import DATABASE, SCHEMA_GOLD, T, WAREHOUSE


def main() -> int:
    t = T()
    product = gold_dir()
    work = Path("gold")
    for name in ("models", "macros", "tests"):
        dest = work / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(product / name, dest)

    host = t.host.replace("https://", "").replace("http://", "")
    if ":" in host:
        hostname, port_s = host.rsplit(":", 1)
        port = int(port_s)
    else:
        hostname, port = host, 443
    env = os.environ.copy()
    env.update(
        {
            "SNOWFLAKE_ACCOUNT": t.account or "test",
            "SNOWFLAKE_USER": "admin",
            "SNOWFLAKE_PASSWORD": t.password,
            "SNOWFLAKE_WAREHOUSE": WAREHOUSE,
            "SNOWFLAKE_DATABASE": DATABASE,
            "SNOWFLAKE_SCHEMA": SCHEMA_GOLD,
            "CONTOSO_SILVER_DATABASE": DATABASE,
            "CONTOSO_SILVER_SCHEMA": "PUBLIC",
            # LAKEHOUSE_ID IS A FABRIC NAME AND THIS IS NOT FABRIC, but core's
            # gold sources.yml spells the silver database
            # `env_var('CONTOSO_SILVER_DATABASE', env_var('LAKEHOUSE_ID'))`,
            # and Jinja evaluates the DEFAULT EAGERLY -- so the fallback is
            # read whether or not the first name is set, and a Fabric-only
            # variable becomes mandatory on every engine.
            #
            # Without it dbt does not reach the engine at all: it fails while
            # PARSING with `Env var required but not provided: 'LAKEHOUSE_ID'`,
            # and gold.py recorded that as a `dialect_gap` -- a plausible wrong
            # reason that stood in this plan for months while gold had in fact
            # never run here. databricks-platform-jobs sets it for the same
            # reason. The real fix belongs in core: stop nesting env_var as a
            # default.
            "LAKEHOUSE_ID": DATABASE,
            "DBT_PROFILES_DIR": str(work.resolve()),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "SNOWFLAKE_HOST": hostname,
            "SNOWFLAKE_PORT": str(port),
        }
    )
    dialect_gap = None
    try:
        subprocess.check_call(
            ["dbt", "run", "--project-dir", str(work), "--profiles-dir", str(work)],
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        dialect_gap = f"dbt-snowflake gold failed on this engine: {exc}"

    # THE CONTRACTS, ACTUALLY EVALUATED. `dbt test` is a SEPARATE invocation
    # from `dbt run` because their exit codes mean different things: models that
    # would not build and guarantees that do not hold are different failures,
    # and one command reports them as one. `call` rather than `check_call` for
    # the same reason the family's other cells use it -- a failing test must
    # exit non-zero while still having WRITTEN the artefact that says which
    # test failed, and check_call would throw before that artefact is read.
    contracts: list[str] = []
    tested = None
    if not dialect_gap:
        test_rc = subprocess.call(
            ["dbt", "test", "--project-dir", str(work), "--profiles-dir", str(work)],
            env=env,
        )
        results = read_run_results(work)
        tested = summarise(results)
        # The glob is the EXPECTATION now, not the answer: these are the
        # contracts core ships, and `check` refuses unless dbt evaluated and
        # passed every one of them.
        contracts = check(
            results,
            {p.stem for p in (product / "tests").glob("*.sql")},
            "gold",
        )
        if test_rc != 0:
            raise ContractError(f"gold: dbt test exited {test_rc} -- {tested}")

    snapshot = {
        "revenue_usd": "0",
        "cancelled_revenue_usd": "0",
        "sale_lines": "0",
        "contracts": contracts,
        "runtime": "snowflake",
        "catalog": DATABASE,
        "engine": "duckdb",
    }
    if dialect_gap:
        snapshot["dialect_gap"] = dialect_gap
    else:
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
