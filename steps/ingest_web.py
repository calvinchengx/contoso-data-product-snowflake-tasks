"""Pull Contoso Web over HTTP and land it in the internal stage.

THE SECOND VENDOR, and the reason there is one. Two vendors is where the work
starts: two credentials that rotate separately, two formats that agree about
nothing, and two customer lists describing overlapping people without either
system knowing the other exists.

WHAT THIS VENDOR SENDS: JSON arrays, and orders arrive NESTED -- one order
carries its own `lines`, because the storefront thinks in baskets. Accounts are
keyed on email, not an id, which is what makes joining this to POS a resolution
problem rather than a join.

LANDED AS JSON TEXT, ONE DOCUMENT PER ROW. This engine's `COPY INTO` accepts
TYPE = JSON, reports `ok` and loads nothing (snowflake-emulator#20), so the
documents ride in as a single CSV column and silver parses them. That is not a
workaround dressed up: loading JSON into one column and parsing it downstream is
the ordinary Snowflake pattern -- it is what VARIANT is for.

WHAT IS NOT DONE HERE IS THE FLATTENING. The page array is a transport
container, so one row per element is the same bronze the other cells hold. The
`lines` array inside each order is DATA, and it stays nested exactly as it
arrived -- exploding it here would move a transform out of silver, where it is
visible and tested, into ingest, where it is neither.
"""

from __future__ import annotations

import csv
import json

import requests
from credentials import resolve
from target import T

from sources import WEB_API, WEB_KEY_SECRET
from stage import WORK, put


FEEDS = [
    ("/api/v2/export/customers", "contoso_web_customers"),
    ("/api/v2/export/products", "contoso_web_products"),
    ("/api/v2/export/orders", "contoso_web_orders"),
]


def fetch(path: str, key: str, page: int | None = None) -> requests.Response:
    params = {} if page is None else {"page": page}
    return requests.get(
        f"{WEB_API}{path}", headers={"X-Api-Key": key}, params=params, timeout=600
    )


def main() -> int:
    t = T()
    # This vendor's own key. Using the POS credential would still land bytes --
    # they are separate processes with separate keys -- and prove nothing.
    api_key = resolve(WEB_KEY_SECRET)
    refused = fetch(FEEDS[0][0], "wrong-key", 1)
    assert refused.status_code == 401, (
        f"Contoso Web accepted a bad API key: {refused.status_code}"
    )

    landed = {}
    for path, subdir in FEEDS:
        dest = WORK / subdir
        dest.mkdir(parents=True, exist_ok=True)
        for stale in dest.glob("*.csv"):
            stale.unlink()

        first = fetch(path, api_key, 1)
        assert first.status_code == 200, (path, first.status_code, first.text[:200])
        total_pages = int(first.headers["X-Total-Pages"])

        docs, parts = 0, 0
        for page in range(1, total_pages + 1):
            r = first if page == 1 else fetch(path, api_key, page)
            assert r.status_code == 200, (path, page, r.status_code, r.text[:200])
            assert int(r.headers["X-Page"]) == page, (r.headers.get("X-Page"), page)
            blob = r.content
            # Each page must be a COMPLETE array. A vendor that split on bytes
            # would hand back something no reader could parse alone, and the
            # failure would surface much later as a parse error naming neither
            # the vendor nor the page.
            assert blob[:1] == b"[" and blob[-1:] == b"]", (
                f"{path} page {page} is not a self-contained JSON array"
            )
            rows = json.loads(blob)
            assert isinstance(rows, list), f"{path} page {page} is not a list"

            out = dest / f"part-{page:04d}.csv"
            with out.open("w", encoding="utf-8", newline="") as fh:
                # QUOTE_ALL, because a JSON document is full of commas and
                # quotes and the whole document has to survive as ONE field.
                w = csv.writer(fh, quoting=csv.QUOTE_ALL)
                w.writerow(["doc"])
                for row in rows:
                    w.writerow([json.dumps(row, separators=(",", ":"))])
            docs += len(rows)
            parts += 1

        over = fetch(path, api_key, total_pages + 1)
        assert over.status_code == 404, (
            f"{path} served page {total_pages + 1} of {total_pages}"
        )
        # UPLOADED, not written into place. The parts above are in this
        # product's own scratch directory; `PUT` is what puts them in the
        # stage, through the driver's file transfer agent -- the same path a
        # real account takes (G46).
        put(t, subdir, sorted(dest.glob("*.csv")))
        landed[subdir] = docs
        print(f"landed {subdir}/ — {parts} part(s), {docs:,} document(s)")

    print(f"Contoso Web: {sum(landed.values()):,} documents across {len(landed)} feed(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
