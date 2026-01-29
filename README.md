### Azure Databricks End-to-End Project (Bronze → Silver → Gold)

This project demonstrates an end-to-end lakehouse pipeline on Azure Databricks using Delta Lake, Unity Catalog, and ABFSS-backed storage. It covers:

- **Bronze**: Streaming ingestion from `abfss://source@...` to `abfss://bronze@...`
- **Silver**: Cleansed, curated Delta tables for `customers`, `orders`, `products`, and `regions`
- **Gold**: Dimensional models
  - `DimCustomers` with SCD Type 1 merge
  - `DimProducts` using a DLT pipeline with expectations and SCD2 via `apply_changes`
  - `FactOrders` upserted from Silver and joined to dimensions


## Prerequisites

- Azure Databricks workspace with Unity Catalog enabled
- DBR runtime with Delta Lake support (e.g., 13.x+)
- Azure Data Lake Storage (ADLS) Gen2 account containing these containers:
  - `source`, `bronze`, `silver`, `gold`
- The notebooks assume a storage account host of `databricksstorage222.dfs.core.windows.net`. Adjust paths if yours differs.
- Unity Catalog objects (catalog and schemas) referenced by the notebooks:
  - Catalog: `databricks_catalog`
  - Schemas: `bronze`, `silver`, `gold`
  - Ensure the schemas exist and your cluster is UC-enabled with permissions to read/write.


## Repository Structure

- `parameters.ipynb`: Produces a list of datasets for orchestration via `dbutils.jobs.taskValues`
- `Bronze_Layer.ipynb`: Parameterized streaming ingestion from `source` to `bronze`
- `Silver_Customers.ipynb`: Cleansing and transformations for customers; writes Delta and registers Silver table
- `Silver_Orders.ipynb`: Cleansing, window functions, writes Delta and registers Silver table
- `Silver_Products.ipynb`: Cleansing, SQL/Python UDF examples, writes Delta and registers Silver table
- `Silver_Regions.ipynb`: Reads from UC bronze table, writes Delta and registers Silver table
- `Gold_Customers.ipynb`: Builds `gold.DimCustomers` with SCD Type 1 merge and `init_load_flag`
- `Gold Products.ipynb`: DLT pipeline for `DimProducts` with expectations and SCD2 `apply_changes`
- `Gold Orders.ipynb`: Builds `gold.FactOrders` and upserts via Delta MERGE


## Data Locations (adjust if needed)

- Source Parquet (landing): `abfss://source@databricksstorage222.dfs.core.windows.net/<dataset>`
- Bronze Streaming Output: `abfss://bronze@databricksstorage222.dfs.core.windows.net/<dataset>`
- Silver Delta Paths:
  - `customers`: `abfss://silver@databricksstorage222.dfs.core.windows.net/customers`
  - `orders`: `abfss://silver@databricksstorage222.dfs.core.windows.net/orders`
  - `products`: `abfss://silver@databricksstorage222.dfs.core.windows.net/products`
  - `regions`: `abfss://silver@databricksstorage222.dfs.core.windows.net/regions`
- Gold Delta Paths:
  - `DimCustomers`: `abfss://gold@databricksstorage222.dfs.core.windows.net/Dimcustomers`
  - `FactOrders`: `abfss://gold@databricksstorage222.dfs.core.windows.net/factorders`


## Unity Catalog Tables Created

- Silver:
  - `databricks_catalog.silver.customers_silver`
  - `databricks_catalog.silver.orders_silver`
  - `databricks_catalog.silver.products_silver`
  - `databricks_catalog.silver.regions_silver`
- Gold:
  - `databricks_catalog.gold.Dimcustomers`
  - `databricks_catalog.gold.factorders`
  - `Live.DimProducts` (within DLT pipeline), promoted as UC table when publishing DLT


## Orchestration Overview

Typical end-to-end run order:

1) Run `parameters.ipynb`
   - Outputs a task value `output_datasets` with `file_name`s: `orders`, `customers`, `products`.

2) For each dataset, run `Bronze_Layer.ipynb` with widget `file_name`
   - Widgets: `dbutils.widgets.text("file_name", "")`
   - Streaming ingest from `abfss://source/.../<file_name>` to `abfss://bronze/.../<file_name>`
   - Uses Auto Loader (`cloudFiles`) and `trigger(once=True)` to process available data

3) Run Silver notebooks
   - `Silver_Customers.ipynb`
     - Drops `_rescued_data`, derives `domains` from `email`, composes `full_name`
     - Writes Delta to Silver and creates `databricks_catalog.silver.customers_silver`
   - `Silver_Orders.ipynb`
     - Cleans schema, timestamps `order_date`, derives `year`
     - Demonstrates window functions (`dense_rank`, `rank`, `row_number`)
     - Writes Delta and creates `databricks_catalog.silver.orders_silver`
   - `Silver_Products.ipynb`
     - Drops `_rescued_data`, registers temp view `products`
     - Demonstrates SQL UDF and Python UDF in UC schema `databricks_catalog.bronze`
     - Adds `discount_price`, writes Delta and creates `databricks_catalog.silver.products_silver`
   - `Silver_Regions.ipynb`
     - Reads UC table `databricks_catalog.bronze.regions`, writes Delta and creates `databricks_catalog.silver.regions_silver`

4) Run Gold dimension/fact
   - `Gold_Customers.ipynb` (SCD Type 1)
     - Widget: `init_load_flag` (0 = incremental from existing gold, 1 = initial full build)
     - De-duplicates by `customer_id`
     - Splits new vs. existing, maintains `create_date`/`update_date`, assigns surrogate `DimCustomerKey`
     - Uses Delta MERGE into `databricks_catalog.gold.Dimcustomers`
   - `Gold Products.ipynb` (DLT pipeline, SCD2)
     - Defines expectations: `product_id IS NOT NULL`, `product_name IS NOT NULL`
     - Ingests streaming from `databricks_catalog.silver.products_silver`
     - Creates streaming view and uses `dlt.apply_changes` to build `DimProducts` as SCD2
     - Deploy as a DLT pipeline with target catalog/schema
   - `Gold Orders.ipynb` (Fact)
     - Reads `silver.orders_silver`, joins to `gold.DimCustomers` and `gold.DimProducts`
     - Drops natural keys, keeps surrogate keys (`DimCustomerKey`, `DimProductKey`)
     - Upserts into `databricks_catalog.gold.factorders` using Delta MERGE


## Running Locally in Databricks

- Attach each notebook to a UC-enabled cluster with access to the ADLS Gen2 account and UC schemas.
- Update any ABFSS paths if your storage account or container names differ from `databricksstorage222`.
- Ensure the UC schemas `bronze`, `silver`, `gold` exist in `databricks_catalog`.

Step-by-step:

1) Open and run `parameters.ipynb`.
2) Open `Bronze_Layer.ipynb`.
   - Set widget `file_name` to one of: `orders`, `customers`, `products`.
   - Run the notebook. Repeat for each dataset.
3) Run `Silver_Customers.ipynb`, `Silver_Orders.ipynb`, `Silver_Products.ipynb`, `Silver_Regions.ipynb`.
4) Run `Gold_Customers.ipynb`.
   - Set `init_load_flag` to `1` for the first run, then `0` for subsequent incrementals.
5) For products dimension, create and run a DLT pipeline using `Gold Products.ipynb`.
6) Run `Gold Orders.ipynb` to maintain `factorders`.


## Scheduling as Jobs

Create a multi-task Databricks Job:

- Task 1: `parameters.ipynb` → stores `output_datasets` in task values
- Task 2..N: `Bronze_Layer.ipynb` (task-per-dataset)
  - Pass `file_name` widget from the task values list
  - Configure retry and trigger-Once cadence
- Task: Silver notebooks (can run in parallel after Bronze completes)
- Task: `Gold Products.ipynb` (DLT pipeline task)
- Task: `Gold_Customers.ipynb` (ensure `init_load_flag` handling for initial run)
- Task: `Gold Orders.ipynb`


## Notable Implementation Details

- Bronze uses Auto Loader with:
  - `format("cloudFiles")`, `option("cloudFiles.format", "parquet")`
  - Checkpoint and path per dataset: `checkpoint_<file_name>`
- Silver tables are registered explicitly via SQL `CREATE TABLE ... USING DELTA LOCATION ...`
- `Gold_Customers.ipynb`:
  - SCD Type 1 into `gold.Dimcustomers` with surrogate keys using `monotonically_increasing_id()` plus max key offset
  - Maintains `create_date`/`update_date` timestamps
- `Gold Products.ipynb` (DLT): uses `@dlt.expect_all` and `dlt.apply_changes(... stored_as_scd_type=2)`
- `Gold Orders.ipynb`: Upsert via `DeltaTable.forName(...).merge(...).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`


## Troubleshooting

- Permission errors: Verify cluster has UC access and storage credentials to ABFSS paths.
- Missing schemas/tables: Create `databricks_catalog.{bronze,silver,gold}` schemas before running.
- Path mismatches: Update storage account/container names in notebooks to match your environment.
- Auto Loader schema evolution: If schema drifts, update `schemaLocation` and consider options for evolution.
- DLT pipeline errors: Ensure the DLT permissions, target UC settings, and that `Gold Products.ipynb` is used as the pipeline notebook.