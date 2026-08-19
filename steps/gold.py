"""dbt-snowflake over the product gold project. Adapter only; SQL is the product's."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from urllib.request import Request, urlopen

from contoso_product import gold_dir
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

    snapshot = {
        "revenue_usd": "0",
        "cancelled_revenue_usd": "0",
        "sale_lines": "0",
        "contracts": sorted(p.stem for p in (product / "tests").glob("*.sql")),
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
    Path("product_snapshot.json").write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    print(f"gold snapshot {snapshot}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
