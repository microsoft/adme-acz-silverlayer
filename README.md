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
5. Review the notebook controls in the Configuration section.
6. Set `RUN_PROFILE = "interactive"` to print effective settings without starting the pipeline.
7. Run the Smoke test bronze access section to validate the bronze table and first selected kind.
8. Set `RUN_PROFILE = "full"` when the configuration is correct.
9. Run the Run Pipeline section, then review Results Summary.
10. Review the generated Silver Layer tables and the `silver_run_info` table.
11. Connect downstream analytics, reporting, or data engineering workloads to the Silver Layer outputs.

## Configure the notebook

The notebook supports direct cell edits and environment variable overrides. Environment variables are useful for scheduled runs or CI/CD-style execution.

| Control | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `RUN_PROFILE` | `ADME_RUN_PROFILE` | `full` | Use `interactive` to review settings without running. Use `full` to execute the pipeline. |
| `KINDS` | `ADME_KINDS` | WellLog and WellboreTrajectory examples | OSDU kind URNs to process. Environment values are comma-separated. |
| `INCREMENTAL` | `ADME_INCREMENTAL` | `False` | Upsert parent rows and replace child rows for changed parent IDs when `true`; otherwise overwrite output tables. |
| `LIMIT` | `ADME_LIMIT` | `0` | Maximum records per kind. `0` means no limit. |
| `OUTPUT_MODE` | `ADME_OUTPUT_MODE` | `normalized` | Use `normalized` for parent and child tables, or `wide` for one table per kind. |
| `DROP_WKT` | `ADME_DROP_WKT` | `True` | Drop WKT geometry columns from output tables. |
| `TABLE_PREFIX` | `ADME_TABLE_PREFIX` | Empty string | Prefix all generated output table names. |
| `bronze_table` | `ADME_BRONZE_TABLE` | `osducatalog` | Bronze Delta table that contains OSDU records. |
| `workspace_id` | `ADME_WORKSPACE_ID` | Runtime value | Fabric workspace identifier. Attach a lakehouse or set this value explicitly. |
| `lakehouse_id` | `ADME_LAKEHOUSE_ID` | Runtime value | Fabric lakehouse identifier. Attach a lakehouse or set this value explicitly. |
| Lakehouse name fallback | `ADME_LAKEHOUSE_NAME` | `osducatalog` | Used only when the notebook must build a OneLake path from a lakehouse name. |

`ADME_REASSEMBLE` is still accepted as a compatibility alias for older scheduled runs when `ADME_OUTPUT_MODE` is not set.

The notebook first tries the attached Fabric lakehouse catalog. If a table is not available through the catalog, it falls back to a OneLake path in this form:

```text
abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}
```

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
| Smoke test bronze access | Reads at most one row for the first configured kind without writing Silver Layer tables. |
| Run Pipeline | Executes the pipeline when `RUN_PROFILE = "full"` or prints next steps when `interactive`. |
| Results Summary | Displays the per-kind result table. |

## Review results

After a successful run, review:

- The parent or reassembled table for each selected kind.
- Generated child tables when `OUTPUT_MODE = "normalized"`.
- `silver_run_info` for run status, record counts, failures, and error messages.
- The Results Summary cell for a quick per-kind view of status, rows, output table name, child count, validation, and error text.

If a kind has no matching bronze records, the run returns `skipped` for that kind.

## Operational guidance

- Use full refresh for initial loads, schema changes, and troubleshooting.
- Use incremental mode only when the bronze source and downstream consumers can tolerate upsert behavior for parent tables and child table replacement for changed parent IDs.
- Use `LIMIT` for smoke tests before processing full OSDU kinds.
- Keep `DROP_WKT = True` unless WKT geometry columns are required by a downstream consumer.
- Use `TABLE_PREFIX` to isolate test outputs from production Silver Layer tables.
- Ensure the runtime can reach the OSDU schema registry before scheduled runs.
- Review `silver_run_info` after each scheduled run to confirm per-kind status and record counts.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Bronze table not found | Confirm the lakehouse is attached, or set `ADME_WORKSPACE_ID`, `ADME_LAKEHOUSE_ID`, and `ADME_BRONZE_TABLE`. |
| No records processed | Confirm the selected `KINDS` values match the `kind` values in the bronze table. |
| Schema load fails | Confirm the runtime can access the OSDU schema registry URL and that the kind URN is valid. |
| Output tables are overwritten unexpectedly | Check that `INCREMENTAL` is set correctly before running with `RUN_PROFILE = "full"`. |
| Output table shape is too normalized | Use `OUTPUT_MODE = "wide"` to create one wide table per selected kind. |
| Results Summary is empty | Confirm the Run Pipeline section executed and that `RUN_PROFILE` was set to `full`. |

## Security

Do not report security vulnerabilities through public GitHub issues. For security reporting guidance, see [SECURITY.md](SECURITY.md).

## License

This project is licensed under the [MIT License](LICENSE.md).
