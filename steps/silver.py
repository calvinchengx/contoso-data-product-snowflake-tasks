"""dbt-snowflake over the product silver project. Adapter only; SQL is the product's.

THE SAME MODELS EVERY OTHER CELL RUNS. Silver used to be seven empty CREATE
TABLEs here (`seed_silver.py`), so gold aggregated nothing and the cell's
numbers were zeros with a reason rather than a result. The models themselves
were always portable in principle -- what was missing was the dialect: three
constructs were written in Spark's spelling because Spark was the only engine
when they were written. Core put them behind macros, and this emulator learned
the Snowflake side of each (DATEADD, GENERATOR/SEQ4, LATERAL FLATTEN,
ARRAY_GENERATE_RANGE), so the project now runs here unchanged.

NOTHING IS COPIED OR RE-STATED. The models come from `silver_dir()` at run
time, exactly as gold comes from `gold_dir()`. A second copy of a model in a
platform repository is how one product quietly becomes two.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from contoso_product import silver_dir
from contracts import check, read_run_results, summarise
from provision import sql
from target import SCHEMA_SILVER, T
from tasks import env_vars_clause, fetch_dbt_output, run_graph, stage_dbt_project

# What bronze called its tables here, against what the models ask for. The
# indirection is the product's: `source('bronze', var('bronze_pos_orders'))`,
# so a platform whose bronze predates the vendor-prefixed scheme supplies its
# own names rather than renaming its tables.
BRONZE_NAMES = {
    "bronze_pos_customers": "bronze_pos_customers",
    "bronze_pos_orders": "bronze_pos_orders",
    "bronze_web_customers": "bronze_web_customers",
    "bronze_web_orders": "bronze_web_orders",
    "bronze_web_products": "bronze_web_products",
    "bronze_ref_product_hierarchy": "bronze_product_hierarchy",
    "bronze_ref_fx_rates": "bronze_fx_rates",
    "bronze_erp_customer_changes": "bronze_erp_changes",
}

COUNTED = [
    "silver_customers",
    "silver_orders",
    "silver_product_hierarchy",
    "silver_fx_daily",
    "silver_party",
    "silver_web_customers",
    "silver_web_order_lines",
]


def main() -> int:
    t = T()
    product = silver_dir()
    work = Path("silver")
    for name in ("models", "macros", "tests"):
        dest = work / name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(product / name, dest)
    shutil.copy(product / "dbt_project.yml", work / "dbt_project.yml")

    # NO CONNECTION SETTINGS HERE ANY MORE. This step used to build a profile
    # out of the target's host, port, account, user and password and hand it to
    # a dbt subprocess. dbt runs INSIDE the account now, so the account supplies
    # its own connection -- there is no host for this step to know, and nothing
    # for it to get wrong.
    stage_dbt_project(t, "silver", work, dbt_vars=BRONZE_NAMES)

    # Bronze landed in the same schema silver writes to, so the source lookup
    # and the model output agree without a second schema to provision. Named
    # rather than defaulted because the models default it to `bronze`, which is
    # not where this platform's bronze is.
    env = env_vars_clause({"DBT_BRONZE_SCHEMA": SCHEMA_SILVER})

    # RUN AND TEST AS TWO NODES, which is Snowflake's own orchestration example
    # and stronger than two subprocesses: `test` runs only if `run` succeeded,
    # and if `run` fails the history says so rather than the contracts being
    # evaluated against models that were never built.
    #
    # SILVER SHIPS ONE SINGULAR TEST and it is the one that matters most here:
    # `silver_orders_never_holds_a_non_positive_quantity` checks that the
    # quarantine split does not leak a row. It is also exactly the shape of test
    # that G29 found rendered by no task in another cell -- a singular test
    # belongs to no model, so anything that iterates models runs it never.
    run_graph(
        t,
        "silver",
        [
            ("run", f"EXECUTE DBT PROJECT silver ARGS='run'{env}"),
            ("test", f"EXECUTE DBT PROJECT silver ARGS='test'{env}"),
        ],
    )

    # WHICH TESTS RAN, from dbt's own record. The graph already carries the
    # verdict -- a failing `dbt test` failed its task and stopped the chain --
    # but not the NAMES, and a step that reports guarantees it cannot show were
    # evaluated is the thing G29 and G41 are about.
    #
    # Fetched from the STAGE, because dbt no longer runs on this host.
    # `read_run_results` asserts `args.which == "test"` on it exactly as before:
    # `run` and `test` write the same filename, and the archive holds whichever
    # ran last.
    fetch_dbt_output(t, "silver", work)
    results = read_run_results(work)
    tested = summarise(results)
    check(results, {p.stem for p in (product / "tests").glob("*.sql")}, "silver")
    print(f"silver contracts: {tested}")

    # COUNTED AFTERWARDS, from the engine rather than from dbt's exit code.
    # dbt reports that it ran the models; only the warehouse can say whether
    # they hold rows, and a silver that builds empty is the failure this step
    # exists to replace.
    metrics = {}
    for table in COUNTED:
        out = sql(t, f"SELECT count(*) FROM {table}")
        if not out.get("success"):
            raise SystemExit(f"silver built but {table} is unreadable: {out}")
        rows = out["data"]["rowset"]
        metrics[table] = int(rows[0][0]) if rows else 0
    Path("silver_metrics.json").write_text(
        json.dumps(metrics, indent=2) + "\n", encoding="utf-8"
    )
    empty = [k for k, v in metrics.items() if v == 0]
    if empty:
        raise SystemExit(
            f"silver built and these tables are empty: {', '.join(empty)}. "
            f"Bronze has rows, so this is a silver failure rather than a missing feed."
        )
    print("silver: " + ", ".join(f"{k.removeprefix('silver_')} {v:,}" for k, v in metrics.items()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
