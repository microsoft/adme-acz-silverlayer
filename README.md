# ADME ACZ Silver Layer

Use the Azure Data Manager for Energy (ADME) Analytics Consumption Zone (ACZ) Silver Layer notebook to transform nested OSDU records into reusable Delta tables for analytics, reporting, and downstream data engineering.

The notebook reads bronze OSDU records from ACZ, resolves OSDU schemas from the ADME schema service, decomposes nested JSON into relational tables, and writes Silver Layer Delta outputs.

## Overview

OSDU records are deeply nested JSON documents with arrays and objects that are difficult to consume directly in lakehouse, SQL, BI, and data engineering workloads. Silver Layer transformation makes those records easier to query and reuse by exposing fields as explicit Delta table columns.

`ADME ACZ Silver Layer.ipynb` prepares OSDU data for Silver Layer consumption by:

- Reading OSDU records from an ACZ bronze Delta table.
- Resolving kind schemas from the configured ADME instance schema service.
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
        +--> ADME schema service lookup
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
- A service principal with permission to call the ADME schema service, including listing schemas with `GET /api/schema-service/v1/schema?latestVersion=False&limit=1`.
- A Key Vault secret containing the service principal client secret, plus notebook permission to read that secret. For interactive testing, device-code authentication is also supported.
- A Fabric notebook runtime with a Synapse PySpark kernel.
- Network access from the runtime to the ADME endpoint, for example `https://contoso.energy.azure.com`.

## Get started

1. Download [`ADME ACZ Silver Layer.ipynb`](ADME%20ACZ%20Silver%20Layer.ipynb) from this repository.
2. Upload or import the notebook into your Microsoft Fabric workspace.
3. Open the uploaded notebook in Microsoft Fabric.
4. Attach the lakehouse that contains the ACZ bronze table, or set the workspace and lakehouse environment overrides.
5. Review the explicit tenant configuration block in the Configuration section.
6. Set or confirm `WORKSPACE_ID`, `LAKEHOUSE_ID`, `BRONZE_TABLE`, `ADME_ENDPOINT`, `ADME_DATA_PARTITION_ID`, ADME authentication settings, `KINDS`, `KIND_LIMITS`, `OUTPUT_MODE`, and `TABLE_PREFIX`.
7. Set `RUN_PROFILE = "interactive"` to print effective settings without starting the pipeline.
8. Run the Setup checklist section to validate tenant configuration, bronze access, ADME schema access, and planned output tables.
9. Run the Smoke test bronze access section to validate the bronze table and first selected kind.
10. Set `RUN_PROFILE = "dry_run"` and run the Run Pipeline section for a write-free preview.
11. Set `ALLOW_OVERWRITE = True` only if a full refresh should replace existing output tables.
12. Set `RUN_PROFILE = "full"` when the configuration is correct, then run the Run Pipeline section.
13. Review the generated Silver Layer tables, `silver_run_info`, and `silver_run_manifest`.
14. Connect downstream analytics, reporting, or data engineering workloads to the Silver Layer outputs.

## Configure the notebook

The notebook is designed to run in a new tenant or workspace by editing one explicit tenant configuration block. Environment variables can still override the same settings for scheduled runs or CI/CD-style execution. `ADME_ENDPOINT`, `ADME_DATA_PARTITION_ID`, and ADME authentication settings are required for schema lookup.

`ADME_TOKEN_SCOPE = "https://management.core.windows.net/.default"` and `ADME_DEVICE_CODE_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"` are static authentication constants in the notebook, not customer-specific tenant configuration. `ADME_TOKEN_SCOPE` is the OAuth scope used to request the ADME access token.

| Control | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `WORKSPACE_ID` | `ADME_WORKSPACE_ID` | Empty string | Fabric workspace identifier. Leave blank to use the attached Fabric lakehouse context. |
| `LAKEHOUSE_ID` | `ADME_LAKEHOUSE_ID` | Empty string | Fabric lakehouse identifier. Leave blank to use the attached Fabric lakehouse context. |
| `BRONZE_TABLE` | `ADME_BRONZE_TABLE` | `osducatalog` | Bronze Delta table that contains OSDU records. |
| `ADME_ENDPOINT` | `ADME_ENDPOINT` | Empty string | Required ADME instance endpoint, for example `https://contoso.energy.azure.com`. Trailing slashes are removed. |
| `ADME_DATA_PARTITION_ID` | `ADME_DATA_PARTITION_ID` | Empty string | Required OSDU data partition id sent as the `data-partition-id` header for schema service calls. |
| `ADME_AUTH_METHOD` | `ADME_AUTH_METHOD` | `SP` | Authentication mode for ADME schema service calls. Use `SP` for service principal client credentials or `DC` for device-code authentication. |
| `ADME_TENANT_ID` | `ADME_TENANT_ID` | Empty string | Required Microsoft Entra tenant id for both `SP` and `DC` authentication. |
| `ADME_SP_CLIENT_ID` | `ADME_SP_CLIENT_ID` | Empty string | Required service principal application client id when `ADME_AUTH_METHOD = "SP"`. |
| `ADME_SP_SECRET_KV_NAME` | `ADME_SP_SECRET_KV_NAME` | Empty string | Required Key Vault name or URL containing the service principal secret when `ADME_AUTH_METHOD = "SP"`. |
| `ADME_SP_SECRET_NAME` | `ADME_SP_SECRET_NAME` | Empty string | Required Key Vault secret name containing the service principal secret when `ADME_AUTH_METHOD = "SP"`. |
| `NOTEBOOK_VERSION` | None | `0.2.0` | Notebook implementation version written to run manifest rows. |
| `RUN_PROFILE` | `ADME_RUN_PROFILE` | `interactive` | Use `interactive` to review settings, `dry_run` to validate and preview planned writes, or `full` to execute the pipeline. |
| `KINDS` | `ADME_KINDS` | WellLog and WellboreTrajectory examples | OSDU kind URNs, all-kinds markers, or wildcard patterns to process. Environment values are comma-separated. |
| `INCREMENTAL` | `ADME_INCREMENTAL` | `False` | Upsert parent rows and replace child rows for changed parent IDs when `true`; otherwise overwrite output tables. |
| `MERGE_KEY_COLUMNS` | `ADME_MERGE_KEY_COLUMNS` | `["id", "version"]` | Columns used for incremental parent/wide upserts and child replacement. Default preserves multiple OSDU record versions per ID. |
| `ALLOW_OVERWRITE` | `ADME_ALLOW_OVERWRITE` | `False` | Allow full refresh to replace existing output tables. Keep disabled for first runs in a new tenant. |
| `LIMIT` | `ADME_LIMIT` | `0` | Maximum records per kind. `0` means no limit. |
| `KIND_LIMITS` | `ADME_KIND_LIMITS` | Empty object | Optional per-kind limits keyed by full kind, table name, or entity name, for example `{"WellLog": 100}`. Environment values can be JSON or `key=value` pairs. |
| `OUTPUT_MODE` | `ADME_OUTPUT_MODE` | `normalized` | Use `normalized` for parent and child tables, or `wide` for one table per kind. |
| `VERSION_STRATEGY` | `ADME_VERSION_STRATEGY` | `merge` | Use `merge` to union multiple schema versions into one logical entity table, or `versioned_tables` to suffix tables by schema version. |
| `MISSING_SCHEMA_MODE` | `ADME_MISSING_SCHEMA_MODE` | `skip` | Use `skip`, `infer`, or `fail` when a kind schema cannot be resolved. `skip` is recommended for schema-correct Silver outputs. |
| `CREATE_EMPTY_CHILD_TABLES` | `ADME_CREATE_EMPTY_CHILD_TABLES` | `True` | Create schema-defined child tables even when the current batch has no rows for those arrays. |
| `DROP_WKT` | `ADME_DROP_WKT` | `True` | Drop WKT geometry columns from output tables. |
| `TABLE_PREFIX` | `ADME_TABLE_PREFIX` | Empty string | Prefix all generated output table names. |
| `PERSIST_SCHEMA_CACHE` | `ADME_PERSIST_SCHEMA_CACHE` | `True` | Read resolved OSDU schemas from a Delta cache when available and persist newly resolved schemas during full runs. Cached rows are reused only when the kind, ADME endpoint, and data partition match the current configuration. |
| `SCHEMA_CACHE_TABLE` | `ADME_SCHEMA_CACHE_TABLE` | `silver_schema_cache` | Delta table used for persisted schema cache rows. |
| `RUN_MANIFEST_TABLE` | `ADME_RUN_MANIFEST_TABLE` | `silver_run_manifest` | Delta table used to record per-kind output manifest rows. |
| `WRITE_OUTPUT_DOCS` | `ADME_WRITE_OUTPUT_DOCS` | `True` | Generate table and column documentation for produced Silver Layer outputs. Documentation rows are buffered with metadata batches during full builds. |
| `OUTPUT_DOCS_MODE` | `ADME_OUTPUT_DOCS_MODE` | `summary` | Use `summary` for one documentation row per table, `full` for column-level documentation, or `off` to skip output docs. |
| `OUTPUT_DOCS_TABLE` | `ADME_OUTPUT_DOCS_TABLE` | `silver_output_documentation` | Delta table used for generated output documentation rows. |
| `CACHE_BRONZE` | `ADME_CACHE_BRONZE` | `True` | Persist the bronze DataFrame during a run so discovery, counts, and kind processing can reuse it. |
| `BRONZE_CACHE_STORAGE_LEVEL` | `ADME_BRONZE_CACHE_STORAGE_LEVEL` | `MEMORY_AND_DISK` | Spark storage level for the bronze cache. |
| `PREFLIGHT_KIND_COUNTS` | `ADME_PREFLIGHT_KIND_COUNTS` | `True` | Compute kind row counts once before processing to avoid per-kind count scans. |
| `BATCH_METADATA_WRITES` | `ADME_BATCH_METADATA_WRITES` | `True` | Buffer `silver_run_info`, `silver_run_manifest`, and output documentation rows and write them in batches. |
| `METADATA_FLUSH_INTERVAL` | `ADME_METADATA_FLUSH_INTERVAL` | `100` | Flush metadata and documentation buffers after this many results, plus a final flush at the end or on exceptions. |
| `SCHEMA_PREFLIGHT` | `ADME_SCHEMA_PREFLIGHT` | `True` | Resolve schemas for all selected kinds before processing groups, falling back to per-kind resolution if preflight fails. |
| Lakehouse name fallback | `ADME_LAKEHOUSE_NAME` | `osducatalog` | Used only when the notebook must build a OneLake path from a lakehouse name. |

`ADME_REASSEMBLE` is still accepted as a compatibility alias for older scheduled runs when `ADME_OUTPUT_MODE` is not set.

The notebook first tries the attached Fabric lakehouse catalog. If a table is not available through the catalog, it falls back to a OneLake path in this form:

```text
abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}
```

When moving to another tenant, update the explicit tenant configuration block first. In most cases, the required changes are `WORKSPACE_ID`, `LAKEHOUSE_ID`, `BRONZE_TABLE`, `KINDS`, `KIND_LIMITS`, `TABLE_PREFIX`, `OUTPUT_MODE`, and `ALLOW_OVERWRITE`.

`KINDS` can include explicit OSDU kind URNs or bronze-driven wildcard selectors. Wildcards are expanded from distinct `kind` values present in the configured bronze table:

```python
KINDS = ["*:*:*:*"]                              # all kinds in bronze
KINDS = ["all"]                                  # all kinds in bronze
KINDS = ["osdu:wks:*:*"]                         # all wks kinds in bronze
KINDS = ["*:wks:work-product-component--Well*:1.*"]  # matching Well* WPC kinds
```

All-kinds and wildcard full runs can create many tables. Use Setup checklist, `RUN_PROFILE = "dry_run"`, `TABLE_PREFIX`, `LIMIT`, `KIND_LIMITS`, and `ALLOW_OVERWRITE` intentionally before running a large wildcard selection.

## Choose an output mode

Use the output mode that matches the table shape required by downstream consumers.

| Mode | Set | Creates | Use when |
| --- | --- | --- | --- |
| Normalized | `OUTPUT_MODE = "normalized"` | Parent table plus child tables for arrays. | You want a normalized relational Silver Layer model with separate tables for repeated fields. |
| Wide | `OUTPUT_MODE = "wide"` | One wide table per kind. | You need a single table per OSDU kind for BI, export, simplified SQL, or other tools that prefer denormalized data. |

Decomposition preserves repeated structures as related tables. Reassembly trades normalization for a wider table shape by expanding struct arrays into indexed columns, pivoting tags, and concatenating primitive arrays.

## Handle multiple schema versions

The default `VERSION_STRATEGY = "merge"` groups concrete kinds by authority, source, and entity, then unions schema versions into one logical table with nullable columns for fields that only exist in some versions. For example, `osdu:wks:master-data--Organisation:1.0.0` and `osdu:wks:master-data--Organisation:1.2.0` both write to `organisation` once, with `schema_version` and `osdu_kind` metadata columns preserving the source version.

Use `VERSION_STRATEGY = "versioned_tables"` if you need strict physical separation by schema version. Versioned mode writes tables such as `organisation__v1_2_0`.

When schemas cannot be resolved from the configured ADME schema service, `MISSING_SCHEMA_MODE = "skip"` records a schema-missing result and continues processing later kinds. Use `fail` when schema lookup failures should stop the run. The notebook does not fall back to the public OSDU data-definitions repository.

## Incremental merge keys

The default `MERGE_KEY_COLUMNS = ["id", "version"]` treats the bronze `version` column as the OSDU record version. This allows multiple versions of the same record `id` to coexist in Silver Layer tables and makes repeated incremental runs idempotent for the same `id + version` pair.

Do not confuse the bronze record `version` column with `schema_version`, which is derived from the kind URN. In incremental mode, parent/wide rows are merged by `MERGE_KEY_COLUMNS`, and child rows are replaced using the same key columns.

## Run the pipeline

The notebook is organized into these executable sections:

| Section | Purpose |
| --- | --- |
| Spark Runtime Configuration | Reuses the Fabric Spark session or creates one outside Fabric, then applies Spark and Delta settings. |
| Configuration | Resolves user controls, environment overrides, workspace, lakehouse, bronze table, ADME schema service settings, and selected kinds. |
| Pipeline Constants | Defines service endpoints, storage scopes, run metadata schema, and result types. |
| Helper Functions | Provides Fabric, OneLake, Delta write, upsert, and bronze-read helpers. |
| Core Decomposition and Reassembly Logic | Resolves schemas, identifies column shapes, decomposes records, builds child tables, and reassembles wide outputs. |
| Pipeline Functions | Processes one or more kinds and records run metadata. |
| Setup checklist | Resolves wildcard kind selectors, validates tenant configuration, first-kind bronze access, ADME schema service access with a one-row schema-list probe, output table names, and planned output tables without writing Silver Layer tables. |
| Smoke test bronze access | Reads at most one row for the first configured kind without writing Silver Layer tables. |
| Run Pipeline | Prints next steps when `interactive`, previews writes when `dry_run`, or executes when `full`. |
| Results Summary | Displays the per-kind result table. |

## Review results

After a successful run, review:

- The parent or reassembled table for each selected kind.
- Generated child tables when `OUTPUT_MODE = "normalized"`.
- `silver_run_info` for run status, record counts, failures, and error messages.
- `silver_run_manifest` for per-kind output mode, version strategy, schema version, schema mode, notebook version, config hash, parent table, child tables, row counts, status, and table prefix.
- `silver_schema_cache` when persisted schema caching is enabled.
- `silver_output_documentation` for generated table and column documentation, including table role, source column, column order, and data type.
- The Results Summary cell for a quick per-kind view of status, rows, output table name, child count, validation, and error text.

If a kind has no matching bronze records, the run returns `skipped` for that kind.

## Operational guidance

- Use full refresh for initial loads, schema changes, and troubleshooting.
- Keep `ALLOW_OVERWRITE = False` until a dry run confirms the planned tables. Set it to `True` only when full refresh should replace existing outputs.
- Use incremental mode only when the bronze source and downstream consumers can tolerate upsert behavior for parent tables and child table replacement for changed parent merge keys. The default merge key is `id + version`.
- Use `LIMIT` for smoke tests before processing full OSDU kinds.
- Use `KIND_LIMITS` to onboard large tenants one kind at a time without changing the global default limit.
- Keep `VERSION_STRATEGY = "merge"` for broad all-kinds runs unless downstream consumers require physically separate tables per schema version.
- Keep `MISSING_SCHEMA_MODE = "skip"` for schema-correct runs that should continue past private or unsupported schemas.
- Keep `CREATE_EMPTY_CHILD_TABLES = True` when downstream consumers need stable child-table contracts.
- Keep `DROP_WKT = True` unless WKT geometry columns are required by a downstream consumer.
- Use `TABLE_PREFIX` to isolate test outputs from production Silver Layer tables.
- Use `RUN_PROFILE = "dry_run"` before the first full run in a new tenant.
- Keep `PERSIST_SCHEMA_CACHE = True` for repeatability when the ADME schema service is temporarily unavailable. Dry runs can read the cache, but only full runs write new cache rows.
- Ensure the runtime can reach the ADME endpoint, retrieve the service principal secret from Key Vault when using `SP`, acquire a token for `ADME_TOKEN_SCOPE`, and list schemas before the first run or when cache misses occur.
- Keep `CACHE_BRONZE = True`, `PREFLIGHT_KIND_COUNTS = True`, and `SCHEMA_PREFLIGHT = True` for large wildcard runs to reduce repeated bronze scans and schema lookups.
- Keep `BATCH_METADATA_WRITES = True` for broad runs. Metadata and output documentation are flushed periodically and at the end, so completed output tables remain durable while reducing small Delta commits.
- Use `OUTPUT_DOCS_MODE = "summary"` for broad runs. Switch to `full` only when column-level generated documentation is required.
- Review `silver_run_info` after each scheduled run to confirm per-kind status and record counts.
- Review `silver_run_manifest` to onboard downstream consumers to the generated tables.
- Review `silver_output_documentation` for generated table and column details.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Bronze table not found | Confirm the lakehouse is attached, or set `ADME_WORKSPACE_ID`, `ADME_LAKEHOUSE_ID`, and `ADME_BRONZE_TABLE`. |
| No records processed | Confirm the selected `KINDS` values match the `kind` values in the bronze table. |
| ADME schema access fails | Confirm `ADME_ENDPOINT`, `ADME_DATA_PARTITION_ID`, `ADME_AUTH_METHOD`, and `ADME_TENANT_ID` are set. For `SP`, confirm `ADME_SP_CLIENT_ID`, `ADME_SP_SECRET_KV_NAME`, and `ADME_SP_SECRET_NAME` are set and the notebook can read the Key Vault secret. Confirm the service principal can list schemas in the configured data partition. |
| Schema load fails | Confirm the runtime can reach the ADME endpoint, MSAL can get a token for the configured scope, and the kind URN exists in the ADME schema service. |
| Private/custom schemas are skipped | Confirm the schema is registered in the ADME data partition and the configured service principal or device-code user is authorized to read it, or use `MISSING_SCHEMA_MODE = "infer"` for best-effort output. |
| Multiple versions collide | Use the default `VERSION_STRATEGY = "merge"` or switch to `versioned_tables` for physical separation. |
| Setup checklist fails | Fix the failed checklist item before running with `RUN_PROFILE = "full"`. |
| Full refresh is blocked by existing tables | Review the listed tables and set `ALLOW_OVERWRITE = True` only if replacing them is intended. |
| Table name validation fails | Update `TABLE_PREFIX`, cache table, or manifest table names to use only letters, numbers, and underscores, starting with a letter or underscore. |
| Output documentation was not written | Confirm `WRITE_OUTPUT_DOCS = True` and that the run reached table write steps for the selected kind. |
| Output tables are overwritten unexpectedly | Check that `INCREMENTAL` is set correctly before running with `RUN_PROFILE = "full"`. |
| Output table shape is too normalized | Use `OUTPUT_MODE = "wide"` to create one wide table per selected kind. |
| Results Summary is empty | Confirm the Run Pipeline section executed and that `RUN_PROFILE` was set to `dry_run` or `full`. |

## Security

Do not report security vulnerabilities through public GitHub issues. For security reporting guidance, see [SECURITY.md](SECURITY.md).

## License

This project is licensed under the [MIT License](LICENSE.md).
