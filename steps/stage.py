"""Getting bytes into the internal stage, the way a real account does.

WHAT THIS REPLACES, and why it was wrong. Ingest used to write its parts
straight into the stage DIRECTORY -- a host path the platform also bind-mounted
into the warehouse -- and `COPY INTO` found them there. That works against this
emulator and CANNOT work against a real Snowflake account, which has no such
directory. Both platform READMEs advertise `SNOWFLAKE_TARGET=emulator|real`,
and `credentials.py` refuses to read a key from disk on a real target precisely
so the code can run against one, so the promise was made in two places by a
pipeline whose ingest half could not honour it (G46).

`PUT` is how a client actually gets bytes into a stage. The driver recognises
it, asks the server where the bytes go, and uploads them through the same file
transfer agent it uses against a real account -- so this path is the real one,
not a local shortcut that happens to work.

THE SCRATCH DIRECTORY IS NOT THE STAGE, and the difference is the whole point.
Ingest writes each part to `work/`, which belongs to this product on whatever
machine it is running on, and then uploads it. Reading a file you just wrote
yourself is fine anywhere; reading THE WAREHOUSE'S STAGE as a filesystem is
what breaks on a real account. Nothing here does the second.

AUTO_COMPRESS IS ON, as it is on a real account, so a part uploaded as
`part-0001.csv` lands as `part-0001.csv.gz`. Consumers name either spelling and
the server resolves it; `staged()` returns what the stage actually holds, so
callers never have to guess which.
"""

from __future__ import annotations

import os
import pathlib

# WHERE INGEST PUTS BYTES BEFORE UPLOADING THEM. Deliberately not called a
# stage: it is this product's own scratch space, and the only thing that reads
# it is the code that wrote it.
WORK = pathlib.Path(
    os.environ.get("PRODUCT_WORK")
    or pathlib.Path(__file__).resolve().parents[1] / "work"
)


def connect(t):
    """A driver connection, because `PUT` is a DRIVER verb.

    The rest of this product talks to `/api/v2/statements` over plain HTTP,
    which is enough for SQL and cannot upload a file: `PUT` is intercepted by
    the connector BEFORE it is sent, and the upload is the connector's own work.
    So this is the one place that needs the real client.
    """
    import snowflake.connector

    host = t.host.replace("https://", "").replace("http://", "")
    hostname, _, port = host.partition(":")
    return snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", "test"),
        user=os.environ.get("SNOWFLAKE_USER", "admin"),
        password=t.password,
        host=hostname,
        port=int(port or 443),
        protocol="http" if t.host.startswith("http://") else "https",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "contoso_warehouse"),
        insecure_mode=not t.tls_verify,
    )


def put(t, subdir: str, parts: list[pathlib.Path]) -> list[str]:
    """Upload each part to `@~/<subdir>/`, returning what the stage now holds.

    ONE `PUT` PER FILE rather than a wildcard. Snowflake accepts
    `PUT file://dir/*.csv`, and naming each file keeps this loop the same shape
    as bronze's `COPY INTO` loop below it -- one statement per part, so a part
    that fails to upload is identifiable rather than being one of several.
    """
    if not parts:
        raise SystemExit(f"nothing to upload for {subdir}: ingest produced no parts")
    con = connect(t)
    try:
        cur = con.cursor()
        for part in parts:
            cur.execute(f"PUT file://{part.resolve()} @~/{subdir}/")
            row = cur.fetchone()
            # The driver reports its own verdict per file. `UPLOADED` is the
            # only one that means the bytes arrived; anything else here would
            # otherwise surface later as an empty table.
            status = row[6] if row and len(row) > 6 else None
            if status != "UPLOADED":
                raise SystemExit(f"PUT {part.name} -> {row}")
    finally:
        con.close()
    return staged(t, subdir)


def staged(t, subdir: str) -> list[str]:
    """What the stage holds under `@~/<subdir>/`, asked of the SERVER.

    `LIST` is how a Snowflake client finds out what is in a stage, and it is the
    replacement for the directory glob that used to do this. The glob read the
    CLIENT's filesystem; this reads the warehouse's, which is the only one that
    is true on both targets.

    Names come back stage-qualified (`~/feed/part-0001.csv.gz`); the leading
    `~/<subdir>/` is stripped so callers deal in part names, as they did before.
    """
    from provision import sql

    out = sql(t, f"LIST @~/{subdir}/")
    if not out.get("success"):
        raise SystemExit(f"LIST @~/{subdir}/ failed: {out.get('message')}")
    rows = (out.get("data") or {}).get("rowset") or []
    prefix = f"~/{subdir}/"
    names = []
    for row in rows:
        name = row[0]
        names.append(name[len(prefix):] if name.startswith(prefix) else name)
    return sorted(names)
