"""Pull Contoso POS over HTTP and land it in the internal stage.

THIS CELL HAD NO SOURCE SYSTEMS AT ALL. It seeded empty silver tables and built
gold over them, so it could report shapes but never evidence -- the family's
numbers are comparable only because every cell pulls the same bytes from the
same pinned simulator.

CUSTOMERS ARE LANDED VERBATIM. The vendor serves delimited text and the stage
takes delimited text, so the bytes that arrive are the bytes `COPY INTO` reads.
The other three vendors are not so lucky: `COPY INTO` accepts TYPE = JSON and
TYPE = PARQUET, reports `ok`, and loads nothing
(snowflake-emulator#20), so they need a landing shape this engine can actually
read. That is a compromise; this one is not, which is why it is first.

PAGED, and the pages are landed as separate parts rather than stitched back
into one file. Reassembling here would put the whole export in this process's
memory -- the exact thing paging removes -- and a directory of parts is what
`COPY INTO` wants anyway: it reads a prefix.
"""

from __future__ import annotations

import csv

import requests
from credentials import resolve

from sources import POS_API, POS_KEY_SECRET
from stage import STAGE


# (operation path, landed subdirectory, part extension). Named from the OpenAPI
# spec's operations, so a spec change that renames a route fails here rather
# than landing an empty file that only bronze will notice.
FEEDS = [
    ("/api/v1/export/customers", "contoso_pos_customers", "csv"),
    # ORDERS ARE JSON LINES, so they land as one JSON document per CSV row --
    # the same shape Contoso Web's pages take, and for the same reason: this
    # engine's TYPE = JSON copy reports `ok` and loads nothing
    # (snowflake-emulator#20). Silver parses the text.
    ("/api/v1/export/orders", "contoso_pos_orders", "jsonl"),
]


def fetch(path: str, key: str, page: int | None = None) -> requests.Response:
    params = {} if page is None else {"page": page}
    return requests.get(
        f"{POS_API}{path}", headers={"X-Api-Key": key}, params=params, timeout=600
    )


def main() -> int:
    api_key = resolve(POS_KEY_SECRET)

    # THE CREDENTIAL IS ENFORCED BY THE VENDOR, and this is where that gets
    # proved. Without its fixture mokapi does not fail: it generates bodies from
    # the OpenAPI schema and answers everything 200, wrong key included. A
    # vendor that accepts `wrong-key` is serving invented data, and everything
    # downstream would be plausible and false.
    refused = fetch(FEEDS[0][0], "wrong-key", 1)
    assert refused.status_code == 401, (
        f"the vendor accepted a bad API key: {refused.status_code} — it is "
        f"serving generated data, not its fixture"
    )

    landed = {}
    for path, subdir, ext in FEEDS:
        dest = STAGE / subdir
        dest.mkdir(parents=True, exist_ok=True)
        for stale in dest.glob("*.csv"):
            stale.unlink()

        first = fetch(path, api_key, 1)
        assert first.status_code == 200, (path, first.status_code, first.text[:200])
        total_pages = int(first.headers["X-Total-Pages"])
        assert total_pages >= 1, (path, total_pages)

        written, parts = 0, 0
        for page in range(1, total_pages + 1):
            r = first if page == 1 else fetch(path, api_key, page)
            assert r.status_code == 200, (path, page, r.status_code, r.text[:200])
            # The vendor says which page this is. Checking it catches a server
            # that ignores the parameter and returns page 1 every time -- which
            # would land the right byte count and the wrong data.
            assert int(r.headers["X-Page"]) == page, (r.headers.get("X-Page"), page)
            blob = r.content
            assert blob, f"{path} page {page} returned an empty body"
            out = dest / f"part-{page:04d}.csv"
            if ext == "csv":
                out.write_bytes(blob)
            else:
                # QUOTE_ALL: a JSON document is full of commas and quotes and
                # has to survive as ONE field.
                docs = [ln for ln in blob.decode("utf-8").splitlines() if ln.strip()]
                with out.open("w", encoding="utf-8", newline="") as fh:
                    w = csv.writer(fh, quoting=csv.QUOTE_ALL)
                    w.writerow(["doc"])
                    for d in docs:
                        w.writerow([d])
            written += len(blob)
            parts += 1

        # One past the end must be refused, or a vendor that answered every page
        # number would look identical to one that paged correctly.
        over = fetch(path, api_key, total_pages + 1)
        assert over.status_code == 404, (
            f"{path} served page {total_pages + 1} of {total_pages}: {over.status_code}"
        )
        landed[subdir] = {"bytes": written, "parts": parts}
        print(f"landed {subdir}/ — {parts} part(s), {written:,} bytes")

    total = sum(v["bytes"] for v in landed.values())
    print(f"Contoso POS: {len(landed)} feed(s), {total:,} bytes into the stage")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
