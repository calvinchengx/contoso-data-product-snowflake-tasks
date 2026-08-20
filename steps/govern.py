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
    # RAISE WITH THE CATALOG'S OWN WORDS. `raise_for_status()` reports
    # "400 Client Error: Bad Request for url: .../domains" and throws the body
    # away -- and the body IS the diagnosis: OpenMetadata answers
    # `[query param domainType must not be null]`, which names the field.
    # Reading the response would have ended a round of guessing immediately.
    if r.status_code >= 400:
        raise SystemExit(f"OpenMetadata refused PUT /{path}: {r.status_code} {r.text[:400]}")
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
                # REQUIRED, AND ONLY ON A FRESH CATALOG. OpenMetadata answers
                # `[query param domainType must not be null]` with a 400, but a
                # PUT over a domain that already exists does not need it -- so
                # this step passed for as long as the catalog outlived a run,
                # and failed the first time the stack came down with its
                # volumes. A field that is mandatory only on first use is one a
                # re-run will never catch, which is why this survived until
                # govern ran here for the first time.
                #
                # `Consumer-aligned` is what the Fabric and Databricks cells
                # already publish. Matching matters more than the taxonomy
                # does: this is a HUMAN catalog, and one product described
                # three ways by three runtimes is the disagreement it exists to
                # remove. Accepted values are Aggregate, Consumer-aligned and
                # Source-aligned; anything else is a 400.
                "domainType": "Consumer-aligned",
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
