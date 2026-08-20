"""Read what dbt actually did, rather than what the repository contains.

WHY THIS EXISTS. `gold.py` used to fill the snapshot's `contracts` field with

    sorted(p.stem for p in (product / "tests").glob("*.sql"))

-- a DIRECTORY LISTING. It named the five ODCS contracts whether or not a single
one had been evaluated, and `make verify` never ran `dbt test` at all, so for the
whole life of this cell the snapshot published five guarantees that nothing had
checked. Run by hand they passed, which is why it survived: the product was
sound and its pipeline could not say so.

That is the family's recurring shape for the third time. G29 was cosmos's
`AFTER_EACH` rendering no task for a singular test; G36 was contract names taken
from a directory listing rather than from `run_results.json`. Both were fixed the
same way, and so is this: dbt writes down what it ran, so read THAT.

THE GLOB DOES NOT DISAPPEAR -- it changes job. It is now the EXPECTATION
(`tests/*.sql` is what core ships) and `run_results.json` is the EVIDENCE. A
contract that core ships and dbt did not evaluate is the failure this module
exists to raise, and it is the one a listing can never notice.
"""

from __future__ import annotations

import json
from pathlib import Path


class ContractError(RuntimeError):
    pass


def read_run_results(work: Path, which: str = "test") -> dict:
    """dbt's own record of the invocation that just finished.

    `args.which` is asserted rather than assumed. `dbt run` and `dbt test` write
    the SAME filename, so reading it without checking would happily report a
    `run` as though it were a `test` -- which is the identical mistake one layer
    down from the one this module fixes.
    """
    path = work / "target" / "run_results.json"
    if not path.is_file():
        raise ContractError(
            f"dbt produced no {path}. It cannot have run: a dbt invocation that "
            f"reaches the adapter writes this file even when it fails."
        )
    rr = json.loads(path.read_text(encoding="utf-8"))
    got = (rr.get("args") or {}).get("which")
    if got != which:
        raise ContractError(
            f"{path} records a `dbt {got}`, not a `dbt {which}`. Reading it "
            f"anyway would report models built as tests passed."
        )
    return rr


def statuses(rr: dict) -> dict[str, str]:
    """node name -> status, for every result dbt recorded."""
    out = {}
    for r in rr.get("results", []):
        name = (r.get("unique_id") or "").split(".")[-1]
        if name:
            out[name] = r.get("status", "unknown")
    return out


def check(rr: dict, expected: set[str], label: str) -> list[str]:
    """Every contract core ships must appear in what dbt evaluated, and pass.

    Returns the contract names, sorted -- which is now a statement about this
    run rather than about the filesystem.
    """
    seen = statuses(rr)
    missing = sorted(expected - seen.keys())
    if missing:
        raise ContractError(
            f"{label}: core ships {len(expected)} singular contract(s) and dbt "
            f"evaluated none of these -- {', '.join(missing)}. A contract that "
            f"is rendered by no task is the defect a passing run hides."
        )
    failed = sorted(n for n in expected if seen[n] != "pass")
    if failed:
        raise ContractError(
            f"{label}: " + ", ".join(f"{n} -> {seen[n]}" for n in failed)
        )
    return sorted(expected)


def summarise(rr: dict) -> str:
    counts: dict[str, int] = {}
    for status in statuses(rr).values():
        counts[status] = counts.get(status, 0) + 1
    total = sum(counts.values())
    body = " ".join(f"{k.upper()}={v}" for k, v in sorted(counts.items()))
    return f"{body} TOTAL={total}"
