# ADME ACZ Silver Layer

Use the Azure Data Manager for Energy (ADME) Analytics Consumption Zone (ACZ) Silver Layer notebook to transform nested OSDU records into reusable Delta tables for analytics, reporting, and downstream data engineering.

The notebook reads bronze OSDU records from ACZ, resolves OSDU schemas from the public schema registry, decomposes nested JSON into relational tables, and writes Silver Layer Delta outputs.

## Overview

OSDU records are deeply nested JSON documents with arrays and objects that are difficult to consume directly in lakehouse, SQL, BI, and data engineering workloads. Silver Layer transformation makes those records easier to query and reuse by exposing fields as explicit Delta table columns.

`ADME ACZ Silver Layer.ipynb` prepares OSDU data for Silver Layer consumption by:

- Reading OSDU records from an ACZ bronze Delta table.
- Resolving kind schemas from the OSDU data definitions repository.
- Flattening scalar and object fields into table columns.
- Splitting array fields into child tables, or optionally reassembling them into one wide table.
- Writing Delta tables into the attached Microsoft Fabric lakehouse or a resolved OneLake path.
- Recording run metadata in `silver_run_info`.

## Architecture

The notebook runs in Microsoft Fabric with a Synapse PySpark kernel.

```text
Azure Data Manager for Energy
        |
        v
Analytics Consumption Zone bronze Delta table
        |
        v
ADME ACZ Silver Layer notebook
        |
        +--> OSDU schema registry lookup
        +--> Type inference and flattening
        +--> Decomposition or reassembly
        |
        v
Silver Layer Delta tables in Fabric or OneLake
        |
        v
Analytics, reporting, and downstream data products
```

## What the notebook creates

The notebook can create normalized parent and child tables or a single reassembled table per OSDU kind.

| Output | Description | Example |
| --- | --- | --- |
| Parent table | One row per OSDU record with scalar and flattened object columns. | `welllog` |
| Child tables | One table per array field, linked to the parent by `id` and ordered with `ordinal` when available. | `welllog___curves` |
| Reassembled table | One wide table per kind when `OUTPUT_MODE = "wide"`. Struct arrays are expanded into indexed columns, tags are pivoted, and primitive arrays are concatenated. | `welllog` |
| Run metadata | Processing status, record counts, timing, and error details for each kind. | `silver_run_info` |

Child tables use a triple-underscore separator: `{parent_table}___{array_field}`. If `TABLE_PREFIX` is set, the prefix is applied to the parent table name and generated child table names.

## Prerequisites

Before you run the notebook, make sure the customer environment has:

- A running Azure Data Manager for Energy instance.
- A configured Analytics Consumption Zone. For setup guidance, see [Enable Analytics Consumption Zone](https://learn.microsoft.com/en-us/azure/energy-data-services/how-to-enable-analytics-consumption-zone?tabs=bash).
- A Microsoft Fabric workspace and lakehouse that can access ACZ bronze Delta tables.
- Permission to read the bronze table and write Silver Layer Delta tables.
- A Fabric notebook runtime with a Synapse PySpark kernel.
- Network access from the runtime to the OSDU schema registry at `https://community.opengroup.org/osdu/data/data-definitions`.

## Get started

1. Download [`ADME ACZ Silver Layer.ipynb`](ADME%20ACZ%20Silver%20Layer.ipynb) from this repository.
2. Upload or import the notebook into your Microsoft Fabric workspace.
3. Open the uploaded notebook in Microsoft Fabric.
4. Attach the lakehouse that contains the ACZ bronze table, or set the workspace and lakehouse environment overrides.
5. Review the explicit tenant configuration block in the Configuration section.
6. Set or confirm `WORKSPACE_ID`, `LAKEHOUSE_ID`, `BRONZE_TABLE`, `KINDS`, `OUTPUT_MODE`, and `TABLE_PREFIX`.
7. Set `RUN_PROFILE = "interactive"` to print effective settings without starting the pipeline.
8. Run the Setup checklist section to validate tenant configuration, bronze access, schema access, and planned output tables.
9. Run the Smoke test bronze access section to validate the bronze table and first selected kind.
10. Set `RUN_PROFILE = "dry_run"` and run the Run Pipeline section for a write-free preview.
11. Set `ALLOW_OVERWRITE = True` only if a full refresh should replace existing output tables.
12. Set `RUN_PROFILE = "full"` when the configuration is correct, then run the Run Pipeline section.
13. Review the generated Silver Layer tables, `silver_run_info`, and `silver_run_manifest`.
14. Connect downstream analytics, reporting, or data engineering workloads to the Silver Layer outputs.

## Configure the notebook

The notebook is designed to run in a new tenant or workspace by editing one explicit tenant configuration block. Environment variables can still override the same settings for scheduled runs or CI/CD-style execution.

| Control | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `WORKSPACE_ID` | `ADME_WORKSPACE_ID` | Empty string | Fabric workspace identifier. Leave blank to use the attached Fabric lakehouse context. |
| `LAKEHOUSE_ID` | `ADME_LAKEHOUSE_ID` | Empty string | Fabric lakehouse identifier. Leave blank to use the attached Fabric lakehouse context. |
| `BRONZE_TABLE` | `ADME_BRONZE_TABLE` | `osducatalog` | Bronze Delta table that contains OSDU records. |
| `NOTEBOOK_VERSION` | None | `0.2.0` | Notebook implementation version written to run manifest rows. |
| `RUN_PROFILE` | `ADME_RUN_PROFILE` | `interactive` | Use `interactive` to review settings, `dry_run` to validate and preview planned writes, or `full` to execute the pipeline. |
| `KINDS` | `ADME_KINDS` | WellLog and WellboreTrajectory examples | OSDU kind URNs to process. Environment values are comma-separated. |
| `INCREMENTAL` | `ADME_INCREMENTAL` | `False` | Upsert parent rows and replace child rows for changed parent IDs when `true`; otherwise overwrite output tables. |
| `ALLOW_OVERWRITE` | `ADME_ALLOW_OVERWRITE` | `False` | Allow full refresh to replace existing output tables. Keep disabled for first runs in a new tenant. |
| `LIMIT` | `ADME_LIMIT` | `0` | Maximum records per kind. `0` means no limit. |
| `OUTPUT_MODE` | `ADME_OUTPUT_MODE` | `normalized` | Use `normalized` for parent and child tables, or `wide` for one table per kind. |
| `DROP_WKT` | `ADME_DROP_WKT` | `True` | Drop WKT geometry columns from output tables. |
| `TABLE_PREFIX` | `ADME_TABLE_PREFIX` | Empty string | Prefix all generated output table names. |
| `PERSIST_SCHEMA_CACHE` | `ADME_PERSIST_SCHEMA_CACHE` | `True` | Read resolved OSDU schemas from a Delta cache when available and persist newly resolved schemas during full runs. |
| `SCHEMA_CACHE_TABLE` | `ADME_SCHEMA_CACHE_TABLE` | `silver_schema_cache` | Delta table used for persisted schema cache rows. |
| `RUN_MANIFEST_TABLE` | `ADME_RUN_MANIFEST_TABLE` | `silver_run_manifest` | Delta table used to record per-kind output manifest rows. |
| Lakehouse name fallback | `ADME_LAKEHOUSE_NAME` | `osducatalog` | Used only when the notebook must build a OneLake path from a lakehouse name. |

`ADME_REASSEMBLE` is still accepted as a compatibility alias for older scheduled runs when `ADME_OUTPUT_MODE` is not set.

The notebook first tries the attached Fabric lakehouse catalog. If a table is not available through the catalog, it falls back to a OneLake path in this form:

```text
abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}
```

When moving to another tenant, update the explicit tenant configuration block first. In most cases, the required changes are `WORKSPACE_ID`, `LAKEHOUSE_ID`, `BRONZE_TABLE`, `KINDS`, `TABLE_PREFIX`, `OUTPUT_MODE`, and `ALLOW_OVERWRITE`.

## Choose an output mode

Use the output mode that matches the table shape required by downstream consumers.

| Mode | Set | Creates | Use when |
| --- | --- | --- | --- |
| Normalized | `OUTPUT_MODE = "normalized"` | Parent table plus child tables for arrays. | You want a normalized relational Silver Layer model with separate tables for repeated fields. |
| Wide | `OUTPUT_MODE = "wide"` | One wide table per kind. | You need a single table per OSDU kind for BI, export, simplified SQL, or other tools that prefer denormalized data. |

Decomposition preserves repeated structures as related tables. Reassembly trades normalization for a wider table shape by expanding struct arrays into indexed columns, pivoting tags, and concatenating primitive arrays.

## Run the pipeline

The notebook is organized into these executable sections:

| Section | Purpose |
| --- | --- |
| Spark Runtime Configuration | Reuses the Fabric Spark session or creates one outside Fabric, then applies Spark and Delta settings. |
| Configuration | Resolves user controls, environment overrides, workspace, lakehouse, bronze table, and selected kinds. |
| Pipeline Constants | Defines service endpoints, storage scopes, run metadata schema, and result types. |
| Helper Functions | Provides Fabric, OneLake, Delta write, upsert, and bronze-read helpers. |
| Core Decomposition and Reassembly Logic | Resolves schemas, identifies column shapes, decomposes records, builds child tables, and reassembles wide outputs. |
| Pipeline Functions | Processes one or more kinds and records run metadata. |
| Setup checklist | Validates tenant configuration, first-kind bronze access, schema registry access, output table names, and planned output tables without writing Silver Layer tables. |
| Smoke test bronze access | Reads at most one row for the first configured kind without writing Silver Layer tables. |
| Run Pipeline | Prints next steps when `interactive`, previews writes when `dry_run`, or executes when `full`. |
| Results Summary | Displays the per-kind result table. |

## Review results

After a successful run, review:

- The parent or reassembled table for each selected kind.
- Generated child tables when `OUTPUT_MODE = "normalized"`.
- `silver_run_info` for run status, record counts, failures, and error messages.
- `silver_run_manifest` for per-kind output mode, notebook version, config hash, parent table, child tables, row counts, status, and table prefix.
- `silver_schema_cache` when persisted schema caching is enabled.
- The Results Summary cell for a quick per-kind view of status, rows, output table name, child count, validation, and error text.

If a kind has no matching bronze records, the run returns `skipped` for that kind.

## Operational guidance

- Use full refresh for initial loads, schema changes, and troubleshooting.
- Keep `ALLOW_OVERWRITE = False` until a dry run confirms the planned tables. Set it to `True` only when full refresh should replace existing outputs.
- Use incremental mode only when the bronze source and downstream consumers can tolerate upsert behavior for parent tables and child table replacement for changed parent IDs.
- Use `LIMIT` for smoke tests before processing full OSDU kinds.
- Keep `DROP_WKT = True` unless WKT geometry columns are required by a downstream consumer.
- Use `TABLE_PREFIX` to isolate test outputs from production Silver Layer tables.
- Use `RUN_PROFILE = "dry_run"` before the first full run in a new tenant.
- Keep `PERSIST_SCHEMA_CACHE = True` for repeatability when the public OSDU schema registry is unavailable or changes unexpectedly. Dry runs can read the cache, but only full runs write new cache rows.
- Ensure the runtime can reach the OSDU schema registry before the first run or when cache misses occur.
- Review `silver_run_info` after each scheduled run to confirm per-kind status and record counts.
- Review `silver_run_manifest` to onboard downstream consumers to the generated tables.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Bronze table not found | Confirm the lakehouse is attached, or set `ADME_WORKSPACE_ID`, `ADME_LAKEHOUSE_ID`, and `ADME_BRONZE_TABLE`. |
| No records processed | Confirm the selected `KINDS` values match the `kind` values in the bronze table. |
| Schema load fails | Confirm the runtime can access the OSDU schema registry URL and that the kind URN is valid. |
| Setup checklist fails | Fix the failed checklist item before running with `RUN_PROFILE = "full"`. |
| Full refresh is blocked by existing tables | Review the listed tables and set `ALLOW_OVERWRITE = True` only if replacing them is intended. |
| Table name validation fails | Update `TABLE_PREFIX`, cache table, or manifest table names to use only letters, numbers, and underscores, starting with a letter or underscore. |
| Output tables are overwritten unexpectedly | Check that `INCREMENTAL` is set correctly before running with `RUN_PROFILE = "full"`. |
| Output table shape is too normalized | Use `OUTPUT_MODE = "wide"` to create one wide table per selected kind. |
| Results Summary is empty | Confirm the Run Pipeline section executed and that `RUN_PROFILE` was set to `dry_run` or `full`. |

## Security

Do not report security vulnerabilities through public GitHub issues. For security reporting guidance, see [SECURITY.md](SECURITY.md).

## License

This project is licensed under the [MIT License](LICENSE.md).
