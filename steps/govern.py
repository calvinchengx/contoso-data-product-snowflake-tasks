"""Publish the product entities to OpenMetadata. DuckDB remains the engine catalog."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path

import time

import requests
from contoso_product.contracts import DOMAIN, METRICS, PRODUCT_NAME, contract_id

OM = os.environ.get("OM_URL", "http://localhost:18586/api/v1").rstrip("/")
OM_USER = os.environ.get("OM_USER", "admin@open-metadata.org")
OM_PASSWORD = os.environ.get("OM_PASSWORD", "admin")

S = requests.Session()


def login() -> None:
    last = None
    for _ in range(90):
        try:
            r = S.post(
                f"{OM}/users/login",
                json={
                    "email": OM_USER,
                    "password": base64.b64encode(OM_PASSWORD.encode()).decode(),
                },
                timeout=60,
            )
            r.raise_for_status()
            S.headers["Authorization"] = f"Bearer {r.json()['accessToken']}"
            return
        except Exception as exc:
            last = exc
            time.sleep(2)
    raise last


def put(path: str, body: dict) -> dict:
    r = S.put(f"{OM}/{path}", json=body, timeout=60)
    r.raise_for_status()
    return r.json() if r.content else {}


def main() -> int:
    catalogued = {
        "product": PRODUCT_NAME,
        "domain": DOMAIN,
        "service": "contoso-snowflake",
        "fqn": "contoso-snowflake.TEST_DB.gold.fct_revenue_summary",
        "contracts": [contract_id("fct_revenue_summary")],
        "metrics": list(METRICS),
    }
    try:
        login()
        put(
            "domains",
            {
                "name": DOMAIN,
                "displayName": "Contoso Commerce",
                "description": "One product, three runtimes (Fabric, Databricks, Snowflake).",
            },
        )
        put(
            "services/databaseServices",
            {
                "name": "contoso-snowflake",
                "serviceType": "Snowflake",
                "connection": {
                    "config": {
                        "type": "Snowflake",
                        "username": "admin",
                        "account": "test",
                        "warehouse": "contoso_warehouse",
                        "database": "TEST_DB",
                        "password": "not-stored",
                    }
                },
            },
        )
        for metric in METRICS:
            put(
                "metrics",
                {
                    "name": metric,
                    "displayName": metric,
                    "description": f"Product metric {metric} on {PRODUCT_NAME}",
                },
            )
    except Exception as exc:
        catalogued["om_error"] = str(exc)
        print(f"openmetadata publish: {exc}")
    Path("catalog.json").write_text(json.dumps(catalogued, indent=2) + "\n", encoding="utf-8")
    print(f"catalogued {PRODUCT_NAME} as {catalogued['fqn']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
