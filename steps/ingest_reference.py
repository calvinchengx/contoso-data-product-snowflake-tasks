"""Pull Contoso Reference over HTTP and land it in the internal stage.

THE FOURTH VENDOR, and the first that is not an operational system. POS, Web and
ERP each record things that happened; this one publishes the definitions they
are all reported against. It is a vendor rather than a table maintained inside
the platform because that is what it is in the business: the group data office
owns it, issues a credential for it, and changes it on its own schedule.

WHY THIS STEP VERIFIES A CHECKSUM WHEN NO OTHER INGEST DOES. Every other feed is
text, so damage in transit announces itself. Parquet does not: it keeps its
`PAR1` magic and footer through byte-level corruption, so a ruined file passes
every cheap check and fails much later inside a reader, naming neither the
transport nor the cause. The vendor publishes the digest of what it sent, and
this compares it before anything else happens.

REWRITTEN AS CSV, and this is the one honest compromise in the ingest path.
`COPY INTO` accepts TYPE = PARQUET, reports `ok` and loads nothing
(snowflake-emulator#20). Both of this vendor's exports are FLAT tables -- a
product rollup and a daily FX table -- so CSV carries every column and every
value without loss. The checksum is still verified against the bytes that
ARRIVED, before the rewrite, so the guard protects the transport it was written
for. The day that copy path works, this step lands the Parquet untouched.
"""

from __future__ import annotations

import hashlib
import io

import requests
from credentials import resolve

from sources import REFERENCE_API, REFERENCE_KEY_SECRET
from stage import STAGE


FEEDS = [
    ("/reference/v1/product-hierarchy", "contoso_reference_product_hierarchy"),
    ("/reference/v1/fx-rates", "contoso_reference_fx_rates"),
]


def fetch(path: str, key: str) -> requests.Response:
    return requests.get(
        f"{REFERENCE_API}{path}", headers={"X-Api-Key": key}, timeout=600
    )


def main() -> int:
    import pyarrow.csv as pv
    import pyarrow.parquet as pq

    api_key = resolve(REFERENCE_KEY_SECRET)
    refused = fetch(FEEDS[0][0], "wrong-key")
    assert refused.status_code == 401, (
        f"Contoso Reference accepted a bad API key: {refused.status_code}"
    )

    landed = {}
    for path, subdir in FEEDS:
        r = fetch(path, api_key)
        assert r.status_code == 200, (path, r.status_code, r.text[:200])
        blob = r.content
        assert blob, f"{path} returned an empty body"

        # Necessary and not sufficient, which is why the digest follows: both
        # markers survive the corruption this guards against, so passing this
        # pair only proves something Parquet-shaped arrived.
        assert blob[:4] == b"PAR1" and blob[-4:] == b"PAR1", (
            f"{path} is not a Parquet file: starts {blob[:4]!r}, ends {blob[-4:]!r}"
        )
        published = r.headers.get("X-Content-SHA256", "")
        assert published, (
            f"{path} served no X-Content-SHA256 — this vendor's format corrupts "
            f"quietly, so an unverifiable body is not usable"
        )
        got = hashlib.sha256(blob).hexdigest()
        assert got == published, (
            f"{path} arrived corrupted: the vendor sent sha256 {published} and "
            f"{len(blob):,} bytes hashing to {got}. Parquet keeps its PAR1 "
            f"markers through this, so nothing downstream would have noticed."
        )

        table = pq.read_table(io.BytesIO(blob))
        dest = STAGE / subdir
        dest.mkdir(parents=True, exist_ok=True)
        for stale in dest.glob("*.csv"):
            stale.unlink()
        pv.write_csv(table, dest / "part-0001.csv")

        landed[subdir] = table.num_rows
        print(
            f"landed {subdir}/ — {len(blob):,} bytes, sha256 verified, "
            f"{table.num_rows:,} rows as CSV"
        )

    assert len(landed) == 2, sorted(landed)
    print(f"Contoso Reference: {len(landed)} feed(s), {sum(landed.values()):,} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
