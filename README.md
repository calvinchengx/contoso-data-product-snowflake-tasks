# contoso-data-product-snowflake-tasks

The Contoso data product on **Snowflake**, orchestrated by **Snowflake Tasks**.

This is the half of the cell a Snowflake team would actually write: four vendor
ingests that land bytes in an internal stage, a bronze built by `COPY INTO`,
dbt profiles for silver and gold, and the target binding that switches between
[`snowflake-emulator`](https://github.com/calvinchengx/snowflake-emulator) and a
real Snowflake account.

Its platform is
[`snowflake-platform-tasks`](https://github.com/calvinchengx/snowflake-platform-tasks),
which stands up the stack and runs this repository:

```bash
make verify PRODUCT=../contoso-data-product-snowflake-tasks
```

## What is here

| | |
|---|---|
| `steps/ingest_*.py` | four vendors, four transports — paged HTTP text and JSON Lines, paged JSON arrays, binary Parquet, and a Postgres change stream over Kafka |
| `steps/bronze.py` | `COPY INTO` from the internal stage, parsing documents at load time so bronze lands the shape silver reads |
| `steps/silver.py` `steps/gold.py` | dbt-snowflake over **core's** projects, materialised from the installed package at run time |
| `steps/target.py` | the emulator-or-real switch, on top of the published `snowflake-target` contract |
| `gold/` `silver/` | dbt **profiles** only. The models arrive from core and are gitignored |

## What is not here, and will not be

Transform SQL, an ODCS contract, or an expected number. Those live once, in
[contoso-data-product](https://github.com/calvinchengx/contoso-data-product),
and this leaf depends on it **by tag**. `test_no_transform_sql_lives_here`
fails if a `.sql` file ever appears — a second copy of a gold model is how "one
data product, many engines" stops being true.

Infrastructure is not here either. There is no compose file and no
`versions.env`; the platform owns those, and `test_the_leaf_holds_no_infrastructure`
keeps it that way.

## Two things the platform hands this product

Neither is reached for — a product does not read into a platform:

- **`data/admin.pat`** — the workspace credential, copied in by `make token`.
- **`CONTOSO_STAGE`** — where the internal stage lives on the host. Ingest
  writes there and the warehouse, a container, reads it back through
  `COPY INTO`. Both halves must name the same directory; `steps/stage.py`
  resolves it once and every step imports it.

## Its history

These steps were `snowflake-platform-tasks/platform/*.py` until the split. A
platform holding its own product made the cell's name a half-truth and made "a
second product can use this platform unchanged" untestable, because there was
no second thing to point it at. Same move as
[`databricks-platform-jobs`](https://github.com/calvinchengx/databricks-platform-jobs)
→ [`contoso-data-product-databricks-jobs`](https://github.com/calvinchengx/contoso-data-product-databricks-jobs).

## Where this fits

- [The family](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/00-family.md) — the matrix and the four tiers
- [The plan](https://github.com/calvinchengx/contoso-data-product/blob/main/docs/01-plan.md) — where every cell stands

Apache-2.0.
