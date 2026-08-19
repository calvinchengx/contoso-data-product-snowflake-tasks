"""This platform's policy on top of the published snowflake-target contract."""

from __future__ import annotations

import os
from pathlib import Path

import snowflake_target

WAREHOUSE = "contoso_warehouse"
DATABASE = "TEST_DB"
SCHEMA_GOLD = "gold"
SCHEMA_SILVER = "PUBLIC"
ROOT = Path(__file__).resolve().parent.parent


def T():
    os.environ.setdefault("SNOWFLAKE_EMULATOR_URL", "http://127.0.0.1:18448")
    os.environ.setdefault("SNOWFLAKE_DATA_DIR", str(ROOT / "data"))
    os.environ.setdefault("SNOWFLAKE_WAREHOUSE", WAREHOUSE)
    os.environ.setdefault("OM_URL", "http://127.0.0.1:18586/api/v1")
    return snowflake_target.target()
