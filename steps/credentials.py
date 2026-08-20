"""How an ingest step gets a vendor's API key. Never from this source tree.

DATABRICKS SECRETS ARE WRITE-ONLY FROM OUTSIDE A JOB, and the emulator is
faithful about it -- `secrets.get_secret` answers:

    secret values are not readable via the REST API; they resolve only into
    job environment variables

That is the real contract, not an emulator limitation, and it decides the shape
of this module. A job on Databricks receives a secret by declaring
`{{secrets/<scope>/<key>}}` in its environment, so the value arrives as an
ENVIRONMENT VARIABLE and the job code reads exactly that. `resolve()` reads
exactly that too, which is why these ingest steps would run unchanged as a job.

THE EMULATOR-ONLY FALLBACK, and why it is not a credential in the repository.
`make verify` runs these steps as host processes, so nothing has resolved the
scope into their environment. The fallback asks THE VENDOR for its own key --
`$SOURCES/_data/<vendor>/.api-key`, the same file the vendor's serve.js reads
to decide what to accept. That is a vendor publishing its credential to the
customer it issued it to, which is what a vendor does; it is not this platform
storing one. Nothing here is committed, and on `SNOWFLAKE_TARGET=real` the
fallback refuses outright rather than reaching for a file that cannot exist.
"""

from __future__ import annotations

import os
from pathlib import Path

# secret name -> the vendor directory that publishes it. The NAME is the
# cross-target address, exactly as it is for the warehouse and the catalog:
# under `real` the same name addresses the customer's own scope entry.
VENDOR_OF = {
    "contoso-pos-api-key": "contoso-pos",
    "contoso-web-api-key": "contoso-web",
    "contoso-reference-api-key": "contoso-reference",
}


def env_name(secret: str) -> str:
    """The environment variable a job would receive this secret in."""
    return secret.upper().replace("-", "_")


def sources_dir() -> Path:
    return Path(
        os.environ.get(
            "SOURCES", Path(__file__).resolve().parents[2] / "contoso-sources"
        )
    )


def published(secret: str) -> str:
    """The vendor's own copy of the key it issues. Emulator only."""
    vendor = VENDOR_OF.get(secret)
    if not vendor:
        raise SystemExit(f"no vendor publishes {secret!r}")
    path = sources_dir() / "_data" / vendor / ".api-key"
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SystemExit(
            f"{secret!r} is not in the environment and the vendor's own key at "
            f"{path} is unreadable ({exc}). Run `make sources` in the sources "
            f"repo, or export {env_name(secret)}."
        ) from exc


def resolve(secret: str) -> str:
    """The value, from the environment first and the vendor second."""
    from target import T

    value = os.environ.get(env_name(secret))
    if value:
        return value
    t = T()
    if not getattr(t, "seed_secrets_allowed", True):
        # `seed_secrets_allowed` is False exactly when the target is real, and
        # it is the right flag to read: both this and seeding are the same
        # question -- may this platform handle a credential itself, or does the
        # customer's own vault own it?
        raise SystemExit(
            f"SNOWFLAKE_TARGET=real: {secret!r} must arrive in the environment "
            f"as {env_name(secret)}, resolved from the customer's secret store. "
            f"This platform will not read a key from disk on a real target."
        )
    return published(secret)
