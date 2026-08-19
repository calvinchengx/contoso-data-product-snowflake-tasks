"""Create the named warehouse. Ids are resolved, never stored in product code."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.request import Request, urlopen

from target import DATABASE, T, WAREHOUSE


def sql(t, statement: str) -> dict:
    host = t.host
    body = json.dumps({"statement": statement, "warehouse": WAREHOUSE, "database": DATABASE}).encode()
    req = Request(
        f"{host}/api/v2/statements",
        data=body,
        headers={"Authorization": f"Bearer {t.password}", "Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(req) as resp:
        return json.loads(resp.read())


def main() -> int:
    t = T()
    created = sql(t, f"CREATE WAREHOUSE {WAREHOUSE}")
    if not created.get("success"):
        raise SystemExit(created)
    state = {
        "warehouse": WAREHOUSE,
        "database": DATABASE,
        "target": t.name,
        "host": t.host,
    }
    Path("state.json").write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    print(f"provisioned warehouse {WAREHOUSE} host={t.host}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
