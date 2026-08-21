"""Run a medallion step as a SNOWFLAKE TASK GRAPH.

THE ORCHESTRATOR THIS CELL IS NAMED FOR. Every other cell in the family is
named for the thing that schedules it -- Airflow 3, Fabric pipelines, Databricks
Jobs -- and each one runs its medallion THROUGH that thing. This one ran its
steps from the platform's Makefile with `uv run`, so the name was the only part
of it that was Snowflake Tasks.

A task body is a SINGLE STATEMENT, and this emulator has no stored procedures,
so a step of N statements becomes N tasks chained with AFTER. That is not a
workaround: `AFTER` is how Snowflake expresses a dependency, and a graph that
stops at its first failure and records what it skipped is exactly what a
pipeline needs from a scheduler.

WHY IT IS WORTH THE INDIRECTION, given the statements are the same either way:
a task graph fails DIFFERENTLY. Run from a Makefile, a failing statement stops
the process and the rest never happen; the operator reads a traceback. Run as a
graph, the failure is recorded against a named node, its dependents are recorded
as SKIPPED with the node that failed them, and TASK_HISTORY answers all of it
afterwards to anything that asks. That record is what the other cells get from
their schedulers, and it is what this cell had no version of.
"""

from __future__ import annotations

from pathlib import Path

from provision import sql
from stage import connect, put
from target import WAREHOUSE

# Where EXECUTE DBT PROJECT leaves a project's output. The emulator reports it
# as OUTPUT_ARCHIVE_URL and names it in the error of a failing run; this is the
# same path, spelled once.
DBT_OUTPUT_PREFIX = "_dbt_output"


def _ok(out: dict, what: str) -> dict:
    if not out.get("success"):
        raise SystemExit(f"{what}: {out.get('message') or out}")
    return out


def run_graph(t, name: str, steps: list[tuple[str, str]]) -> None:
    """Create `steps` as a chain of tasks, run it, and report from TASK_HISTORY.

    `steps` is [(node, statement)], run in the order given. Each node depends on
    the one before it, which is what makes a failure stop the rest rather than
    running them against a table that was never built.
    """
    if not steps:
        raise SystemExit(f"{name}: a graph with no statements would report success for doing nothing")

    names = [f"{name}_{node}" for node, _ in steps]

    # CREATE OR REPLACE, so a re-run does not inherit yesterday's body. A task
    # left over from a previous shape of this step would run happily and be
    # invisible in the diff.
    for i, (stmt, task_name) in enumerate(zip([s for _, s in steps], names, strict=True)):
        after = f"AFTER {names[i - 1]} " if i else ""
        _ok(
            sql(t, f"CREATE OR REPLACE TASK {task_name} WAREHOUSE = {WAREHOUSE} {after}AS {stmt}"),
            f"create task {task_name}",
        )

    # RESUMED, and every dependent needs it. This emulator runs a graph whether
    # or not the dependents are resumed; real Snowflake does not, and a step
    # that only works here is the thing this platform exists to avoid. Resuming
    # is correct on both, so it is what this does.
    for task_name in names[1:]:
        _ok(sql(t, f"ALTER TASK {task_name} RESUME"), f"resume {task_name}")

    root = names[0]
    out = sql(t, f"EXECUTE TASK {root}")

    # THE HISTORY IS THE REPORT, read whether the run passed or failed -- on a
    # failure it is the only thing that says WHICH node failed and what was
    # skipped because of it, and reading it only on success would surface it on
    # every run except the ones that need it.
    history = sql(t, "SELECT name, state, error_message FROM TABLE(information_schema.task_history())")
    rows = (history.get("data") or {}).get("rowset", []) if history.get("success") else []
    mine = {r[0].upper(): (r[1], r[2]) for r in rows if r[0].upper().startswith(name.upper() + "_")}

    failed = [(n, m) for n, (st, m) in mine.items() if st == "FAILED"]
    skipped = [n for n, (st, _) in mine.items() if st == "SKIPPED"]
    if failed or not out.get("success"):
        detail = "; ".join(f"{n}: {(m or '').strip()[:200]}" for n, m in failed)
        raise SystemExit(
            f"{name}: the task graph failed. {detail or (out.get('message') or out)}"
            + (f" Skipped: {', '.join(sorted(skipped))}." if skipped else "")
        )

    # EVERY NODE RAN, asserted rather than assumed. A graph that executed only
    # its root reports the same success as one that ran all of it -- and a task
    # missing from the history is not "not started yet" here, it is a node the
    # run never reached.
    missing = [n for n in names if n.upper() not in mine]
    ran = [n for n in names if mine.get(n.upper(), ("", ""))[0] == "SUCCEEDED"]
    if missing or len(ran) != len(names):
        raise SystemExit(
            f"{name}: EXECUTE TASK reported success but the history does not show "
            f"every node succeeding. Missing: {missing or 'none'}; "
            f"succeeded {len(ran)} of {len(names)}."
        )
    print(f"{name}: {len(names)} task(s) through Snowflake Tasks, all SUCCEEDED")


def stage_dbt_project(t, name: str, work: Path, dbt_vars: dict) -> None:
    """Put a dbt project into the stage and register it as an account object.

    THE PROJECT BECOMES A SNOWFLAKE OBJECT, which is what `EXECUTE DBT PROJECT`
    runs. dbt does not run on this host at all any more: the project is uploaded
    to the internal stage, `CREATE DBT PROJECT` names it, and every invocation
    after that is a STATEMENT -- which is what lets a task graph chain `run` and
    `test` with AFTER, and is Snowflake's own documented way to orchestrate dbt.

    VARS GO IN dbt_project.yml, NOT ON A COMMAND LINE. Snowflake's `ARGS` is a
    single-quoted SQL string and `--vars '{"a": "b"}'` cannot be spelled inside
    one -- the inner quotes are what the JSON needs and the outer ones cannot
    nest. Carrying them in the project is an ordinary way to deploy dbt and it
    is the way that works on both targets; snowflake-emulator#54 records the
    gap rather than hiding it.

    ENV_VARS GO ON EXECUTE, not here -- see `env_vars_clause`. Snowflake keeps
    a project's environment in its env.yml and lets a run override it; there is
    no ENV_VARS on CREATE, and the emulator refuses one by name.
    """
    project_yml = work / "dbt_project.yml"
    lines = project_yml.read_text(encoding="utf-8").split("\n")
    if dbt_vars:
        # MERGED INTO THE PRODUCT'S OWN vars, not appended as a second block.
        #
        # silver's dbt_project.yml already declares `country_variants`, which
        # core refuses to let drift from `contoso_product.COUNTRY`. A second
        # `vars:` key would be the last one to win in YAML and would silently
        # delete it -- so this adds keys INSIDE the existing block, and refuses
        # only on a real collision, where a platform would genuinely be
        # overriding a decision the product made.
        at = next((i for i, line in enumerate(lines) if line.rstrip() == "vars:"), None)
        if at is None:
            lines += ["", "vars:"]
            at = len(lines) - 1
        # OVERRIDING A DEFAULT IS THE MECHANISM, not a mistake. The product
        # ships a default name for each bronze table and a platform says what it
        # actually called them -- which is exactly what `--vars` did before,
        # where a CLI var beats a project one. So a colliding SCALAR is
        # replaced.
        #
        # A colliding key whose value is a NESTED block is refused instead:
        # `country_variants` is a map that core refuses to let drift from
        # `contoso_product.COUNTRY`, and replacing its first line would leave
        # its children orphaned under a scalar -- a silent corruption of the one
        # thing the product guards hardest.
        end = len(lines)
        for i, line in enumerate(lines[at + 1 :], at + 1):
            if line and not line.startswith(" "):
                end = i
                break
        remaining = dict(dbt_vars)
        out = []
        for i, line in enumerate(lines[at + 1 : end], at + 1):
            key = line.strip().split(":", 1)[0] if line.startswith("  ") and ":" in line else None
            top = key if (key and not line.startswith("   ")) else None
            if top and top in remaining:
                nested = i + 1 < end and lines[i + 1].startswith("    ")
                if nested:
                    raise SystemExit(
                        f"{name}: the product sets {top} as a nested block in "
                        f"dbt_project.yml; this platform cannot override it without "
                        f"orphaning what is under it."
                    )
                out.append(f"  {top}: {remaining.pop(top)}")
                continue
            out.append(line)
        out += [f"  {k}: {v}" for k, v in sorted(remaining.items())]
        lines[at + 1 : end] = out
    project_yml.write_text("\n".join(lines), encoding="utf-8")

    # THROUGH THE DRIVER'S PUT, which is the only route into a stage that exists
    # on both targets. Writing into the stage directory would be reaching into a
    # filesystem only the emulator has -- the same mistake G46 found in bronze's
    # discovery, one layer over.
    files = [p for p in sorted(work.rglob("*")) if p.is_file() and "target" not in p.parts]
    if not files:
        raise SystemExit(f"{name}: no project files to upload")
    for path in files:
        rel = path.relative_to(work).parent.as_posix()
        sub = f"{name}_project" if rel == "." else f"{name}_project/{rel}"
        put(t, sub, [path], auto_compress=False)

    _ok(sql(t, f"DROP DBT PROJECT IF EXISTS {name}"), f"drop dbt project {name}")
    _ok(
        sql(t, f"CREATE DBT PROJECT {name} FROM '@~/{name}_project'"),
        f"create dbt project {name}",
    )
    print(f"{name}: {len(files)} project file(s) staged, registered as a DBT PROJECT")


def fetch_dbt_output(t, name: str, work: Path) -> Path:
    """Bring a dbt project's run_results.json back from the stage.

    THE EVIDENCE OUTLIVES THE RUN, which is the point of it being in the stage
    rather than in a result set. `EXECUTE DBT PROJECT` leaves dbt's own record
    at the path it reports as OUTPUT_ARCHIVE_URL, and a FAILING run has no
    result set to carry anything -- so the artefacts of the run worth
    investigating are exactly the ones an inline answer would have lost.

    Fetched with GET, through the driver's file transfer agent, because that is
    the route that exists on both targets. It lands where `read_run_results`
    already looks, so the contract check below it is unchanged: the names it
    publishes come from what dbt EVALUATED, not from a glob of the files that
    happen to be on disk.
    """
    target = work / "target"
    target.mkdir(parents=True, exist_ok=True)
    stale = target / "run_results.json"
    # REMOVED FIRST. A GET that fetches nothing leaves yesterday's answer in
    # place, and read_run_results would believe it -- publishing a verdict from
    # a run that is not the one just made.
    if stale.exists():
        stale.unlink()

    con = connect(t)
    try:
        cur = con.cursor()
        cur.execute(f"GET @~/{DBT_OUTPUT_PREFIX}/{name.upper()}/run_results.json file://{target.resolve()}")
        rows = cur.fetchall()
    finally:
        con.close()
    if not rows or rows[0][2] != "DOWNLOADED":
        raise SystemExit(f"{name}: dbt's run_results could not be fetched from the stage: {rows}")
    if not stale.exists():
        raise SystemExit(
            f"{name}: GET reported DOWNLOADED and {stale} is not there — "
            f"the client and the server disagree about where the stage is."
        )
    return work


def env_vars_clause(env: dict) -> str:
    """The ENV_VARS a run overrides its project's environment with.

    ON EXECUTE, because that is where Snowflake puts it. UPPERCASE and
    DBT_-prefixed, because that is what it enforces -- "every key ... must be
    prefixed with DBT_ ... every key must be UPPERCASE", checked on every run.
    That constraint is why core's gold reads `DBT_SILVER_DATABASE` at all: the
    name it used before could not be supplied here by any route, so gold could
    run on every engine in this family except this one.

    Answering these matters more than it looks. `env_var('DBT_BRONZE_SCHEMA')`
    defaults to `bronze`, which is not where this platform's bronze lives, so a
    value that never arrives builds silver against the wrong source with
    nothing failing.
    """
    bad = sorted(k for k in env if k != k.upper() or not k.startswith("DBT_"))
    if bad:
        raise SystemExit(
            f"ENV_VARS keys must be UPPERCASE and DBT_-prefixed; Snowflake refuses "
            f"the rest and dbt would never see them: {bad}"
        )
    pairs = ", ".join(f"{k} = '{v}'" for k, v in sorted(env.items()))
    return f" ENV_VARS = ({pairs})" if pairs else ""
