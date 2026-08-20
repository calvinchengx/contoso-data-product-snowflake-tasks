"""Leaf-boundary tests. No Docker, no emulator, no platform.

A leaf is the half of a cell a Snowflake team would actually write, and it has
to stand on its own: clone it, resolve it, run these. That is DoD item 1 in the
family plan, and it is checkable here in seconds rather than only in the
platform's acceptance run.

THE PIPELINE ITSELF IS NOT RUN HERE. That needs the emulator, four vendors and
a stack, and it belongs to `snowflake-platform-tasks`, which checks this
repository out and runs `make verify PRODUCT=...` against it.
"""

from __future__ import annotations

import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parent.parent
STEPS = ROOT / "steps"


def test_emulator_only_in_target_resolver():
    """One place knows the emulator's address, and it is not every step.

    `target.py` is the emulator-or-real switch; gold, silver and govern carry
    the dbt and OpenMetadata wiring that needs the host spelled out. Anything
    else naming a port or a credential file is a step that would have to be
    edited to point at a real Snowflake account.
    """
    allowed = {
        STEPS / "target.py",
        STEPS / "gold.py",
        STEPS / "silver.py",
        STEPS / "govern.py",
        ROOT / "gold" / "profiles.yml",
    }
    hits = []
    for p in STEPS.glob("*.py"):
        if p in allowed:
            continue
        text = p.read_text(encoding="utf-8")
        if "127.0.0.1:18448" in text or "admin.pat" in text:
            hits.append(p.name)
    assert hits == []


def test_product_is_imported_not_restated():
    """Gold's SQL comes from core at run time; a copy here is a second product."""
    gold = (STEPS / "gold.py").read_text(encoding="utf-8")
    assert "from contoso_product import gold_dir" in gold
    assert "decimal(19,4)" not in gold


def test_silver_is_imported_not_restated():
    silver = (STEPS / "silver.py").read_text(encoding="utf-8")
    assert "from contoso_product import silver_dir" in silver


def test_no_transform_sql_lives_here():
    """The README's promise, as a test.

    Every model, macro and singular test is materialised from
    `contoso-data-product` at run time into `gold/` and `silver/`, both
    gitignored. A committed `.sql` file is the moment "one data product, many
    engines" stops being true, and it would arrive as a convenience.
    """
    # ASK GIT, not the filesystem. `gold/` and `silver/` are working
    # directories: the steps materialise core's models into them at run time and
    # .gitignore covers them, so a tree walk here reports the models this cell
    # is SUPPOSED to fetch. The promise is that none of it is ever COMMITTED,
    # and only git can answer that.
    out = subprocess.run(
        ["git", "ls-files", "*.sql"], cwd=ROOT, capture_output=True, text=True, check=True
    )
    tracked = [line for line in out.stdout.splitlines() if line.strip()]
    assert tracked == [], f"transform SQL has been committed to the leaf: {tracked}"


def test_bronze_is_sql_not_spark():
    """This cell builds bronze with SQL, because Snowflake has no DataFrames.

    The invariant that matters is that bronze is `COPY INTO` from a stage --
    what a Snowflake team writes -- and not a Spark session smuggled in to make
    this cell resemble the others.
    """
    bronze = STEPS / "bronze.py"
    assert bronze.exists(), "bronze.py is gone -- this cell lands one deliberately"
    src = bronze.read_text(encoding="utf-8")
    assert "COPY INTO" in src, "bronze must load through the stage"
    for banned in ("pyspark", "SparkSession", "databricks.connect"):
        assert banned not in src, f"bronze reaches for Spark ({banned})"


def test_the_stage_is_resolved_in_one_place():
    """The coupling the split created, guarded rather than remembered.

    The stage is shared state: ingest writes the vendors' bytes into it and the
    warehouse -- a container the PLATFORM runs -- reads them back through
    `COPY INTO`. While the steps and the compose file lived in one repository,
    both spelled it `<repo>/stages` and agreed by accident. They are now in two,
    so the platform passes PRODUCT_STAGE and mounts exactly that path.

    A step that derives the path itself would work in a lone clone and write
    files the warehouse cannot see under a platform that mounts somewhere else
    -- surfacing as an EMPTY BRONZE rather than as an error. So the shape is
    the assertion: `stage.py` resolves it, and every step imports it.
    """
    assert (STEPS / "stage.py").is_file()
    offenders = []
    for p in STEPS.glob("*.py"):
        if p.name == "stage.py":
            continue
        src = p.read_text(encoding="utf-8")
        if "STAGE" not in src:
            continue
        if "from stage import STAGE" not in src:
            offenders.append(p.name)
    assert offenders == [], (
        "these steps name a stage without importing the resolved one: "
        + ", ".join(offenders)
    )


def test_the_leaf_holds_no_infrastructure():
    """A leaf carries no infrastructure, the way its platform carries no Contoso.

    That is `00-family.md`'s split line, and it is what makes "this product runs
    on a real Snowflake account" a claim anyone can test: there is nothing here
    that assumes an emulator is running in Docker next door.
    """
    forbidden = [
        "docker-compose.yml",
        "compose/docker-compose.yml",
        "versions.env",
        "Dockerfile",
    ]
    present = [f for f in forbidden if (ROOT / f).exists()]
    assert present == [], f"infrastructure has arrived in the leaf: {present}"


def test_no_dependency_comes_from_a_sibling_checkout():
    """This repository must clone and build on its own.

    `path = "../..."` is invisible to everyone who already has the siblings on
    disk and fails for everyone who does not -- which is the whole population a
    leaf claims to serve. It had a second cost in the platform this code came
    from: with no version pin, it was the one consumer a core release could not
    reach, and v0.1.1 and v0.2.0 both went past it without anything to bump.
    """
    proj = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    offenders = [
        line.strip()
        for line in proj.splitlines()
        if "path = " in line and "../" in line and not line.lstrip().startswith("#")
    ]
    assert not offenders, (
        "a dependency resolves from a sibling checkout, so a lone clone cannot "
        "build: " + str(offenders)
    )


def test_both_wheels_come_from_a_tagged_release():
    """Core and the client arrive by tag, not from `main`.

    A leaf that floats is a leaf whose numbers cannot be reproduced later.
    """
    proj = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    for name, repo in (
        ("contoso-data-product", "contoso-data-product"),
        ("snowflake-target", "snowflake-emulator"),
    ):
        marker = f"{repo}/releases/download/v"
        assert marker in proj, f"{name} does not install from a tagged release"


def test_both_dbt_steps_run_their_tests():
    """`dbt run` proves models build. It proves nothing about a guarantee.

    Until this was fixed, `make verify` invoked `dbt run` twice and `dbt test`
    never, while the snapshot published five contract names taken from a
    directory listing. The cell reported guarantees nothing had checked.
    """
    for step in ("gold.py", "silver.py"):
        src = (STEPS / step).read_text(encoding="utf-8")
        assert '"test"' in src and "dbt" in src, f"{step} never invokes dbt test"
        assert "read_run_results" in src, (
            f"{step} does not read dbt's own record of what it ran -- a step "
            f"that trusts its own exit code cannot say WHICH tests ran"
        )


def test_the_snapshot_contract_list_is_not_a_directory_listing():
    """The exact defect, named so it cannot come back as a convenience.

    `sorted(p.stem for p in (product / "tests").glob("*.sql"))` assigned
    straight into the snapshot is a list of FILES presented as a list of
    guarantees. The glob is still used -- as the expectation that
    `contracts.check` holds dbt's results against -- so the assertion is about
    where its output goes, not about whether it exists.
    """
    src = (STEPS / "gold.py").read_text(encoding="utf-8")
    body = src[src.index('"contracts"') : src.index('"contracts"') + 200]
    assert "glob" not in body, (
        "the snapshot's contract list is a directory listing again -- it must "
        "come from run_results.json, so that naming a contract means it ran"
    )


def test_run_results_is_read_with_args_which_asserted():
    """dbt run and dbt test write the SAME filename.

    Reading it without checking `args.which` reports models built as tests
    passed -- the identical mistake one layer below the one being fixed.
    """
    src = (STEPS / "contracts.py").read_text(encoding="utf-8")
    assert 'args"' in src and '"which"' in src or 'get("which")' in src, (
        "contracts.py does not assert which invocation wrote run_results.json"
    )


def test_a_contract_that_did_not_run_is_a_failure(tmp_path):
    """Checked against the failure, not only the happy path.

    The whole point is to notice a contract that was never evaluated. A guard
    that only ever sees passing input is the thing it is guarding against.
    """
    import sys

    sys.path.insert(0, str(STEPS))
    from contracts import ContractError, check, read_run_results

    ran = {
        "args": {"which": "test"},
        "results": [
            {"unique_id": "test.contoso_gold.money_is_never_stored_as_float", "status": "pass"}
        ],
    }
    assert check(ran, {"money_is_never_stored_as_float"}, "gold") == [
        "money_is_never_stored_as_float"
    ]

    # shipped by core, evaluated by nobody
    try:
        check(ran, {"money_is_never_stored_as_float", "revenue_summary_loses_no_revenue"}, "gold")
    except ContractError as exc:
        assert "revenue_summary_loses_no_revenue" in str(exc)
    else:
        raise AssertionError("a contract that never ran was reported as met")

    # evaluated and failed
    failed = {
        "args": {"which": "test"},
        "results": [
            {"unique_id": "test.contoso_gold.money_is_never_stored_as_float", "status": "fail"}
        ],
    }
    try:
        check(failed, {"money_is_never_stored_as_float"}, "gold")
    except ContractError as exc:
        assert "fail" in str(exc)
    else:
        raise AssertionError("a failing contract was reported as met")

    # a `dbt run` masquerading as a `dbt test`
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "run_results.json").write_text(
        json.dumps({"args": {"which": "run"}, "results": []}), encoding="utf-8"
    )
    try:
        read_run_results(tmp_path)
    except ContractError as exc:
        assert "not a `dbt test`" in str(exc)
    else:
        raise AssertionError("a dbt run was accepted as a dbt test")


def test_the_readme_inventory_matches_the_pinned_core():
    """The README's product list must be what this leaf's pin actually contains.

    A generated list that falls behind is worse than none: a reader trusts it
    BECAUSE it looks generated. The check lives in the core, so all seven leaves
    ask the same question of their own pin, and it fails here, in the repository
    that has to fix it.

    Regenerate with:  python -m contoso_product.show --markdown
    """
    from pathlib import Path

    from contoso_product import show

    ok, message = show.check(Path(__file__).resolve().parent.parent / "README.md")
    assert ok, message
