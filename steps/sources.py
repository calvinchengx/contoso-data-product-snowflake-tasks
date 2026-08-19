"""Where the source systems live.

Contoso POS, Web, Reference and ERP are NOT Snowflake. They are the vendors a
pipeline pulls from, and in production they are a real REST endpoint, a real
Postgres and a real Kafka broker. Keeping their addresses out of target.py is
the same discipline as keeping the emulator out of the platform.

THE DEFAULTS ARE THIS PLATFORM'S PUBLISHED PORTS (see scripts/sources.py), not
the vendors' own, so three stacks can run side by side on one machine: the
Fabric platforms hold 180xx, Databricks 181xx, and this one 182xx.
"""

from __future__ import annotations

import os

# Contoso POS -- delimited text and JSON Lines, paged.
POS_API = os.environ.get("POS_API_URL", "http://localhost:18290")
POS_KEY_SECRET = os.environ.get("POS_KEY_SECRET", "contoso-pos-api-key")

# Contoso Web -- JSON arrays, orders nested over their lines.
WEB_API = os.environ.get("WEB_API_URL", "http://localhost:18291")
WEB_KEY_SECRET = os.environ.get("WEB_KEY_SECRET", "contoso-web-api-key")

# Contoso Reference -- binary Parquet, and its own credential.
REFERENCE_API = os.environ.get("REFERENCE_API_URL", "http://localhost:18292")
REFERENCE_KEY_SECRET = os.environ.get("REFERENCE_KEY_SECRET", "contoso-reference-api-key")

# Contoso ERP -- a relational source, captured by CDC.
ERP_HOST = os.environ.get("ERP_HOST", "localhost")
ERP_PORT = os.environ.get("ERP_PORT", "55436")
ERP_DB = os.environ.get("ERP_DB", "erp")
ERP_USER = os.environ.get("ERP_USER", "contoso")
REDPANDA = os.environ.get("REDPANDA_BOOTSTRAP", "localhost:19096")
ERP_TOPIC = os.environ.get("ERP_TOPIC", "contoso.erp.customer")
