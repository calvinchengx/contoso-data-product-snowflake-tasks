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
import os
import shutil
import subprocess
from pathlib import Path

from contoso_product import silver_dir
from provision import sql
from target import DATABASE, SCHEMA_SILVER, T, WAREHOUSE

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
            "SNOWFLAKE_SCHEMA": SCHEMA_SILVER,
            # Bronze landed in the same schema silver writes to, so the source
            # lookup and the model output agree without a second schema to
            # provision. Named rather than defaulted because the models default
            # it to `bronze`, which is not where this platform's bronze is.
            "DBT_BRONZE_SCHEMA": SCHEMA_SILVER,
            "DBT_PROFILES_DIR": str(work.resolve()),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "SNOWFLAKE_HOST": hostname,
            "SNOWFLAKE_PORT": str(port),
        }
    )

    subprocess.check_call(
        [
            "dbt",
            "run",
            "--project-dir",
            str(work),
            "--profiles-dir",
            str(work),
            "--vars",
            json.dumps(BRONZE_NAMES),
        ],
        env=env,
    )

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
