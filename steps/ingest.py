"""Land every vendor the product reads, into the internal stage.

FOUR VENDORS, FOUR TRANSPORTS, and this file runs them in order rather than
doing any of the work: paged delimited text and JSON Lines over HTTP, paged JSON
arrays over HTTP, binary Parquet over HTTP, and a Postgres change stream carried
by Kafka. Each step is its own module because each vendor is its own failure --
a wrong key, a mangled binary body, a short change stream -- and one function
would report all of them as "ingest failed".

WHAT THIS CELL DID BEFORE: nothing. It seeded seven empty silver tables and
built a gold star over them, which could report shapes but never evidence. The
family's numbers are comparable only because every cell pulls the same bytes
from the same pinned simulator.
"""

from __future__ import annotations

import ingest_erp_cdc
import ingest_pos
import ingest_reference
import ingest_web

STEPS = [
    ("Contoso POS", ingest_pos),
    ("Contoso Web", ingest_web),
    ("Contoso Reference", ingest_reference),
    ("Contoso ERP", ingest_erp_cdc),
]


def main() -> int:
    for name, step in STEPS:
        print(f"--- {name} ---")
        rc = step.main()
        if rc != 0:
            return rc
    print("all four vendors landed in the stage")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
