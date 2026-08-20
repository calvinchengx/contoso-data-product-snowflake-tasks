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

## What the product contains

The SQL is not here. It lives in the core so seven leaves cannot drift into
seven versions of it, and that costs you a click, so this list gives it back.
`make show-product` copies the same files into `product/` where you can open
them; the block below is generated from the pinned package and a test fails
when it falls behind.

<!-- BEGIN product inventory: python -m contoso_product.show --markdown -->

The product is [`contoso-data-product`](https://github.com/calvinchengx/contoso-data-product/tree/v0.5.0) at **v0.5.0**, the version this repository pins. It is not vendored here: these files live there and are staged locally by `make show-product`.

**silver**: 8 models, 1 singular test

- [`silver_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_customers.sql)
- [`silver_fx_daily`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_fx_daily.sql)
- [`silver_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_orders.sql)
- [`silver_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_party.sql)
- [`silver_product_hierarchy`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_product_hierarchy.sql)
- [`silver_quarantine_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_quarantine_orders.sql)
- [`silver_web_customers`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_web_customers.sql)
- [`silver_web_order_lines`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/models/silver_web_order_lines.sql)

Assertions over silver, each failing the build on its own:

- [`silver_orders_never_holds_a_non_positive_quantity`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/silver/tests/silver_orders_never_holds_a_non_positive_quantity.sql)

**gold**: 9 models, 5 singular tests

- [`dim_country`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_country.sql)
- [`dim_customer`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_customer.sql)
- [`dim_date`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_date.sql)
- [`dim_party`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_party.sql)
- [`dim_product`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/dim_product.sql)
- [`fct_daily_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/fct_daily_revenue.sql)
- [`fct_orders`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/fct_orders.sql)
- [`fct_revenue_summary`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/fct_revenue_summary.sql)
- [`fct_sales`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/models/fct_sales.sql)

Assertions over gold, each failing the build on its own:

- [`both_selling_systems_reach_the_pack`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/both_selling_systems_reach_the_pack.sql)
- [`every_country_resolves_to_the_dimension`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/every_country_resolves_to_the_dimension.sql)
- [`fiscal_year_is_not_the_calendar_year`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/fiscal_year_is_not_the_calendar_year.sql)
- [`money_is_never_stored_as_float`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/money_is_never_stored_as_float.sql)
- [`revenue_summary_loses_no_revenue`](https://github.com/calvinchengx/contoso-data-product/blob/v0.5.0/src/contoso_product/gold/tests/revenue_summary_loses_no_revenue.sql)

<!-- END product inventory -->

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
- **`PRODUCT_STAGE`** — where the internal stage lives on the host. Ingest
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
