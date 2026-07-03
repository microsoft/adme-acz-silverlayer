# ADME ACZ Silver Layer

Use the Azure Data Manager for Energy (ADME) Analytics Consumption Zone (ACZ) Silver Layer notebook to turn nested OSDU records into reusable Delta tables for analytics, reporting, and downstream data engineering.

The notebook reads bronze OSDU records from ACZ, unwraps Storage record envelopes in the bronze `data` payload, resolves schemas from the ADME schema service, flattens nested JSON, and writes Silver Layer Delta outputs into Microsoft Fabric or OneLake.

## What the notebook creates

| Output | Description | Example |
| --- | --- | --- |
| Parent table | One row per OSDU record with scalar and flattened object columns. | `osdu_wks_welllog` |
| Child tables | One table per array field, linked to the parent by `id` and ordered with `ordinal` when available. | `osdu_wks_welllog___curves` |
| Reassembled table | One wide table per kind when `OUTPUT_MODE = "wide"`. | `osdu_wks_welllog` |
| Run metadata and quality issues | Processing status, row counts, timing, output manifest, run commit status, data-quality issues, and optional generated documentation. | `silver_run_info`, `silver_run_status`, `silver_data_quality_issues` |

Parent tables include the normalized kind authority and source, followed by the entity name, for example `osdu_wks_welllog` or `data_wks_file_generic`. Child tables use `{parent_table}___{array_field_path}` with the normalized full array field path, which keeps nested array outputs distinct. If `TABLE_PREFIX` is set, the prefix is applied to all generated output table names.

## Architecture

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

## Prerequisites

Before running the notebook, confirm that the customer environment has:

- A running Azure Data Manager for Energy instance.
- A configured Analytics Consumption Zone. For setup guidance, see [Enable Analytics Consumption Zone](https://learn.microsoft.com/en-us/azure/energy-data-services/how-to-enable-analytics-consumption-zone?tabs=bash).
- A Microsoft Fabric workspace and lakehouse that can access the ACZ bronze Delta table.
- Permission to read the bronze table and write Silver Layer Delta tables.
- An identity that can call the ADME schema service, including `GET /api/schema-service/v1/schema?latestVersion=False&limit=1`.
- For direct notebook runs, a service principal with its client secret stored in Key Vault. Device-code authentication is supported for interactive testing.
- A Fabric notebook runtime with a Synapse PySpark kernel.
- Network access from the runtime to the ADME endpoint, for example `https://contoso.energy.azure.com`.

## Quick start

1. Download [`ADME ACZ Silver Layer.ipynb`](ADME%20ACZ%20Silver%20Layer.ipynb) and import it into Microsoft Fabric.
2. Attach the lakehouse that contains the ACZ bronze table, or set `WORKSPACE_ID` and `LAKEHOUSE_ID`.
3. Edit only the **customer settings** at the top of the notebook configuration cell.
4. Keep `RUN_PROFILE = "interactive"` and run the Configuration, Setup checklist, and Smoke test sections.
5. Set `RUN_PROFILE = "dry_run"` and run the Run pipeline section to preview planned outputs without writing Silver tables.
6. Set `RUN_PROFILE = "full"` when the dry run looks correct. Set `ALLOW_OVERWRITE = True` only when replacing existing output tables is intended.
7. Review the generated Silver tables, `silver_run_info`, and `silver_run_manifest`.

## Customer configuration

Most customers only need the settings in this section. The remaining notebook settings are advanced defaults for incremental refreshes, metadata tables, schema caching, and performance tuning.

| Decision | Settings | Guidance |
| --- | --- | --- |
| Fabric target | `WORKSPACE_ID`, `LAKEHOUSE_ID` | Leave both blank when the Fabric lakehouse is attached. Set them only for scheduled or detached runs that need explicit OneLake resolution. |
| Bronze source | `BRONZE_TABLE` | ACZ bronze Delta table containing OSDU records. Default is `osducatalog`. Inactive rows are excluded by default using the bronze `isActive` column. |
| ADME schema service | `ADME_ENDPOINT`, `ADME_DATA_PARTITION_ID` | Required for schema lookup. Use the customer ADME endpoint and OSDU data partition id. |
| Authentication | `ADME_AUTH_METHOD`, `ADME_TENANT_ID`, `ADME_SP_CLIENT_ID`, `ADME_SP_SECRET_KV_NAME`, `ADME_SP_SECRET_NAME`, `ADME_MANAGED_IDENTITY_CLIENT_ID` | Use `MI` for production orchestration with the runner's managed identity. Use `SP` for direct notebook runs with a Key Vault stored client secret. Use `DC` only for interactive validation. |
| Scope | `KINDS`, `EXCLUDED_KINDS`, `LIMIT`, `KIND_LIMITS` | Start with one or two explicit kinds and a small limit. Widen to wildcard or all-kinds selections after the dry run is clean, and use exclusions for kind families that should never be processed. |
| Output shape | `OUTPUT_MODE` | Use `normalized` for parent and child tables. Use `wide` when consumers need one denormalized table per kind. |
| Table safety | `TABLE_PREFIX`, `ALLOW_OVERWRITE` | Use a test prefix for onboarding. Keep overwrite disabled until the planned tables have been reviewed. |
| Run stage | `RUN_PROFILE` | Use `interactive` to review settings, `dry_run` to validate and preview writes, and `full` to execute. |

Recommended first-run shape:

```python
WORKSPACE_ID = ""              # leave blank when a lakehouse is attached
LAKEHOUSE_ID = ""              # leave blank when a lakehouse is attached
BRONZE_TABLE = "osducatalog"

ADME_ENDPOINT = "https://contoso.energy.azure.com"
ADME_DATA_PARTITION_ID = "opendes"
ADME_AUTH_METHOD = "SP"
ADME_TENANT_ID = "<tenant-id>"
ADME_SP_CLIENT_ID = "<application-client-id>"
ADME_SP_SECRET_KV_NAME = "<key-vault-name-or-url>"
ADME_SP_SECRET_NAME = "<secret-name>"
ADME_MANAGED_IDENTITY_CLIENT_ID = ""  # optional; set only for user-assigned MI

RUN_PROFILE = "interactive"
KINDS = ["osdu:wks:work-product-component--WellLog:1.4.0"]
EXCLUDED_KINDS = []
LIMIT = 10
KIND_LIMITS = {}
OUTPUT_MODE = "normalized"
TABLE_PREFIX = "test_"
ALLOW_OVERWRITE = False
```

Environment variables can override the same settings for scheduled execution. Use environment overrides for automation; edit the notebook values for first-time interactive onboarding.

By default, Silver Layer processing transforms only bronze rows where `isActive == true`; rows where `isActive` is `false` or `null` are excluded. Set the advanced `INCLUDE_INACTIVE_RECORDS = True` control, or `ADME_INCLUDE_INACTIVE_RECORDS=true`, only when inactive records should also be transformed.

## When to use advanced settings

Leave advanced settings at their defaults unless one of these needs applies.

| Need | Settings | Default guidance |
| --- | --- | --- |
| Include inactive bronze records | `INCLUDE_INACTIVE_RECORDS` | Keep `False` so only `isActive == true` records are transformed. Set `True` only when inactive records should be included. |
| Scheduled incremental refresh | `WRITE_MODE`, `MERGE_KEY_COLUMNS`, `INCREMENTAL_WATERMARK_COLUMN`, `INCREMENTAL_WATERMARK_MODE`, `INCREMENTAL_STATE_TABLE` | Start with `WRITE_MODE = "full_refresh"`. For ACZ `osducatalog`, `ingestTime` is the default source-change watermark for `WRITE_MODE = "upsert"`. |
| Multiple schema versions | `VERSION_STRATEGY` | Keep `versioned_tables` for physical separation by schema version. Use `merge` only when consumers want one logical table across versions. |
| Missing private schemas | `MISSING_SCHEMA_MODE` | Keep `skip` for schema-correct outputs that continue past unresolved kinds. Use `infer` only when best-effort output is preferred for unresolved private schemas. Use `fail` when missing schemas should stop the run. |
| Stable child-table contracts | `CREATE_EMPTY_CHILD_TABLES` | Keep enabled when downstream consumers expect schema-defined child tables even when the current batch has no rows. |
| Geometry columns | `DROP_WKT` | Leave disabled unless downstream consumers want WKT geometry columns removed. |
| Schema repeatability | `PERSIST_SCHEMA_CACHE`, `SCHEMA_CACHE_TABLE` | Keep enabled so full runs persist resolved schemas and later runs can tolerate transient schema-service misses. |
| Metadata, quality, and documentation | `RUN_MANIFEST_TABLE`, `RUN_STATUS_TABLE`, `DATA_QUALITY_CHECKS`, `DATA_QUALITY_ISSUES_TABLE`, `DATA_QUALITY_MAX_EXAMPLES`, `WRITE_OUTPUT_DOCS`, `OUTPUT_DOCS_MODE`, `OUTPUT_DOCS_TABLE` | Keep manifest, run status, and data-quality checks enabled. Use `OUTPUT_DOCS_MODE = "summary"` for broad runs and `full` only when column-level generated documentation is required. |
| Large wildcard runs | `CACHE_BRONZE`, `PREFLIGHT_KIND_COUNTS`, `BATCH_METADATA_WRITES`, `METADATA_FLUSH_INTERVAL`, `SCHEMA_PREFLIGHT`, `SCHEMA_FETCH_PARALLELISM`, `OUTPUT_WRITE_PARALLELISM`, `WIDE_MAX_CARDINALITY_CAP` | Keep defaults for broad runs to reduce repeated bronze scans, parallelize bounded schema lookups, batch metadata/data-quality commits, limit small Delta commits, and cap wide-mode array expansion. `OUTPUT_WRITE_PARALLELISM = "auto"` uses the Fabric workspace SKU when available and falls back to `1`; set an integer only after validating that the Fabric Spark pool can run concurrent independent Delta writes. Limited runs using `LIMIT` or `KIND_LIMITS` automatically bypass bronze caching and count preflight so small samples do not materialize the full bronze slice. |
| Transient ADME schema failures | `ADME_SCHEMA_TIMEOUT_SECONDS`, `ADME_SCHEMA_RETRY_TOTAL`, `ADME_SCHEMA_RETRY_BACKOFF_SECONDS`, `ADME_SCHEMA_RETRY_STATUS_CODES` | Keep retries enabled. Tune only for scheduled runs that process many kinds or encounter throttling. |

`ADME_TOKEN_SCOPE = "https://management.core.windows.net/.default"` and `ADME_DEVICE_CODE_CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"` are static authentication constants in the notebook, not customer-specific tenant settings.

`ADME_INCREMENTAL` and `ADME_REASSEMBLE` are still accepted as compatibility aliases for older scheduled runs. Prefer `ADME_WRITE_MODE` and `ADME_OUTPUT_MODE` for new runs.

## Selecting kinds

`KINDS` can include explicit OSDU kind URNs or bronze-driven wildcard selectors. Wildcards are expanded from distinct `kind` values in the configured bronze table:

```python
KINDS = ["osdu:wks:work-product-component--WellLog:1.4.0"]
KINDS = ["*:*:*:*"]                                # all kinds in bronze
KINDS = ["all"]                                    # all kinds in bronze
KINDS = ["osdu:wks:*:*"]                           # all wks kinds in bronze
KINDS = ["*:wks:work-product-component--Well*:1.*"]  # matching Well* WPC kinds
```

Use `EXCLUDED_KINDS` to remove exact kinds or wildcard matches after `KINDS` is expanded. For scheduled runs, set `ADME_EXCLUDED_KINDS` to the same comma-separated selector format:

```python
KINDS = ["*:*:*:*"]
EXCLUDED_KINDS = ["osdu:wks:reference*:*"]
```

All-kinds and wildcard full runs can create many tables. Use the Setup checklist, `RUN_PROFILE = "dry_run"`, `TABLE_PREFIX`, `LIMIT`, `KIND_LIMITS`, and `ALLOW_OVERWRITE` intentionally before running a large wildcard selection.

## Choose an output mode

| Mode | Set | Creates | Use when |
| --- | --- | --- | --- |
| Normalized | `OUTPUT_MODE = "normalized"` | Parent table plus child tables for arrays. | You want a relational Silver Layer model with separate tables for repeated fields. |
| Wide | `OUTPUT_MODE = "wide"` | One wide table per kind. | You need a single table per OSDU kind for BI, export, simplified SQL, or tools that prefer denormalized data. |

Normalized output preserves repeated structures as related tables. Wide output expands struct arrays into indexed columns, pivots tags, and concatenates primitive arrays.

## Handle multiple schema versions

The default `VERSION_STRATEGY = "versioned_tables"` keeps schema versions physically separate. Versioned mode writes tables such as `osdu_wks_organisation__v1_2_0`.

Use `VERSION_STRATEGY = "merge"` when consumers prefer one logical table across schema versions. Merge mode groups concrete kinds by authority, source, and entity, then unions schema versions into one table with nullable columns for fields that only exist in some versions. `schema_version` and `osdu_kind` metadata columns preserve the source version.

When schemas cannot be resolved from the configured ADME schema service, `MISSING_SCHEMA_MODE = "skip"` records a schema-missing result and continues processing later kinds. Set `MISSING_SCHEMA_MODE = "infer"` only when best-effort output is acceptable: the notebook infers a temporary schema from the selected bronze payload, writes `schema_mode = "inferred"` in the manifest, and continues without claiming schema-certified output. Set `MISSING_SCHEMA_MODE = "fail"` when unresolved schemas should stop the run.

The notebook does not fall back to the public OSDU data-definitions repository.

Schema parsing supports common OSDU and private-schema variants, including `definitions`, `$defs`, local references through either form, nullable type arrays such as `["null", "string"]`, and nullable `anyOf`/`oneOf` branches.

## Authentication guidance

For production scheduling, set `ADME_AUTH_METHOD = "MI"` and use a managed identity through a supported Fabric pipeline notebook activity connection, workspace identity, or equivalent orchestrator. Leave `ADME_MANAGED_IDENTITY_CLIENT_ID` blank for the system-assigned identity; set it only when the runner should use a specific user-assigned managed identity. Grant that identity only the ADME/OSDU entitlement groups and data-plane permissions required to read schemas and source data.

Direct notebook execution keeps `ADME_AUTH_METHOD = "SP"` as the default because managed identity token acquisition is not assumed inside every interactive Fabric notebook runtime. Store the service principal secret in Key Vault, rotate it regularly, and grant the notebook only secret read access. Use `ADME_AUTH_METHOD = "DC"` only for interactive validation.

## Upsert mode, watermarks, and merge keys

The default `MERGE_KEY_COLUMNS = ["id", "version"]` treats the bronze `version` column as the OSDU record version. This allows multiple versions of the same record `id` to coexist in Silver Layer tables and makes repeated upsert runs idempotent for the same `id + version` pair.

Do not confuse the bronze record `version` column with `schema_version`, which is derived from the kind URN. In `WRITE_MODE = "upsert"`, parent or wide rows are merged by `MERGE_KEY_COLUMNS`, and child rows are replaced using the same key columns.

By default, `INCREMENTAL_WATERMARK_COLUMN = "ingestTime"` uses the ACZ bronze update timestamp to prune incremental upsert runs to affected concrete kinds before schema preflight and group processing. Active changed rows are transformed and upserted; rows explicitly marked `isActive = false` hard-delete matching Silver parent/wide and child rows by `MERGE_KEY_COLUMNS` so the default Silver outputs remain active-only. If `INCLUDE_INACTIVE_RECORDS = True`, inactive rows are included in the transformed Silver outputs instead of being hard-deleted. Use `INCREMENTAL_WATERMARK_MODE = "required"` when a scheduled job must fail rather than process all selected rows if the watermark column is missing.

When watermark filtering is active, do not set `LIMIT` or `KIND_LIMITS`; the notebook rejects that combination because advancing a persistent watermark after a limited batch can skip unprocessed records. Watermark filtering intentionally includes rows at the previous maximum watermark value so late-arriving records with the same watermark are reprocessed safely through idempotent upserts and deletes.

## Run the pipeline

The notebook is organized into these executable sections:

| Section | Purpose |
| --- | --- |
| Spark runtime configuration | Reuses the Fabric Spark session or creates one outside Fabric, then applies Spark and Delta settings. |
| Configuration | Resolves customer settings, environment overrides, workspace, lakehouse, bronze table, ADME schema service settings, and selected kinds. |
| Pipeline constants | Defines service endpoints, storage scopes, run metadata schema, and result types. |
| Helper functions | Provides Fabric, OneLake, Delta write, upsert, and bronze-read helpers. |
| Core decomposition and reassembly logic | Resolves schemas, identifies column shapes, decomposes records, builds child tables, and reassembles wide outputs. |
| Pipeline functions | Processes one or more kinds and records run metadata. |
| Setup checklist | Validates tenant configuration, bronze access, ADME schema access, output table names, and planned output tables without writing Silver tables. |
| Smoke test bronze access | Reads at most one row for the first configured kind without writing Silver tables. |
| Run pipeline | Prints next steps when `interactive`, previews writes when `dry_run`, or executes when `full`. |
| Results summary | Displays the per-kind result table. |

## Development layout

The customer-facing artifact remains `ADME ACZ Silver Layer.ipynb`. Customer runs in Microsoft Fabric should not need any helper `.py` files deployed beside the notebook.

Reusable helpers live under `src/adme_acz_silverlayer/` so configuration parsing, naming, JSON Schema compatibility behavior, Fabric/OneLake boundary behavior, ADME schema URL/auth helpers, bronze filter decisions, and notebook hygiene can be tested directly outside Fabric. Local development tooling is intentionally separate from the Fabric runtime contract: helper modules make development and tests easier, while the committed notebook stays self-contained for import into Fabric.

Use the notebook sync command before committing notebook changes:

```powershell
python scripts\sync_notebook.py --check --summary
python scripts\sync_notebook.py
python -m unittest discover -s tests -q
```

`--check` validates the notebook format, required section order, absence of code-cell outputs, and the self-contained contract that customer notebook code does not import `adme_acz_silverlayer` at runtime. Running without `--check` normalizes removable execution artifacts such as cell outputs and execution counts.

## Onboarding assets

The `samples/` folder contains generic, placeholder-based assets for customer onboarding:

| Asset | Purpose |
| --- | --- |
| `samples/config/interactive_sp.json` | First-run interactive profile using service principal authentication and a small limit. |
| `samples/config/dry_run_validation.json` | Write-free validation profile for expanding kind coverage. |
| `samples/config/scheduled_full_mi.json` | Production-style scheduled profile using managed identity and upsert mode. |
| `samples/fabric_pipeline_parameters.json` | Generic Fabric pipeline notebook activity parameter values. |
| `samples/synthetic_bronze_records.json` | Tiny synthetic bronze-like records for local shape review or sample table creation. |

Replace every placeholder before running in a customer environment. Do not add tenant ids, secrets, or customer-specific values to committed sample files.

## Review results

After a successful run, review:

- The parent or reassembled table for each selected kind.
- Generated child tables when `OUTPUT_MODE = "normalized"`.
- `silver_run_info` for run status, record counts, failures, schema access details, watermark settings, and stage timings.
- `silver_run_manifest` for produced table names, output mode, write mode, schema versions, row counts, config hash, active-record filter behavior, and status.
- `silver_run_status` for run-level publish state. Treat only the latest `committed` status for a `run_id` as a completed multi-table publish; `started` means the run began writing, and `failed` means outputs may be partial.
- `silver_data_quality_issues` for capped examples of non-blocking quality findings such as missing/null/duplicate merge keys, malformed JSON-looking values, and columns not present in the resolved schema. `quality_status` and `quality_issue_count` are also written to run metadata.
- `silver_schema_cache` when persisted schema caching is enabled.
- `silver_incremental_state` when watermark-based source filtering is enabled.
- `silver_output_documentation` for generated table and column documentation.
- The Results summary cell for a quick per-kind view of status, rows, output table name, child count, validation, and error text.

If a kind has no matching bronze records, the run returns `skipped` for that kind.

## Monitoring examples

```sql
SELECT status, COUNT(*) AS kinds, SUM(records_processed) AS rows
FROM silver_run_info
GROUP BY status;

SELECT kind, status, error_type, error_message
FROM silver_run_info
WHERE status <> 'success';

SELECT kind, parent_table, child_table_count, write_mode, watermark_column, watermark_mode
FROM silver_run_manifest
WHERE run_id = '<run-id>';

SELECT run_id, status, status_time, table_names, error_message
FROM silver_run_status
WHERE run_id = '<run-id>'
ORDER BY status_time DESC;

SELECT kind, check_name, severity, COUNT(*) AS examples
FROM silver_data_quality_issues
WHERE run_id = '<run-id>'
GROUP BY kind, check_name, severity;
```

Review high `duration_seconds` values and `stage_timings_json` to identify expensive phases such as bronze scans, schema preflight, group processing, metadata flush, or output documentation flush.

## Sizing, cost, and performance guidance

For smoke tests and first tenant validation, use `RUN_PROFILE = "dry_run"`, a small `LIMIT`, narrow `KINDS`, a test `TABLE_PREFIX`, and `OUTPUT_DOCS_MODE = "summary"`.

For medium onboarding runs, process one domain or entity family at a time with `KIND_LIMITS`, keep broad-run performance defaults enabled, and review `silver_run_manifest` before widening selection.

For large wildcard or all-kinds runs, expect cost and runtime to be driven by bronze scans, schema lookups, array-heavy child table expansion, wide-table column growth, Delta commits, and output documentation volume. Prefer normalized output for array-heavy entities unless downstream consumers require a wide table.

Schema preflight fetches unresolved schemas with bounded parallelism (`SCHEMA_FETCH_PARALLELISM`, default `4`) and reuses retry-enabled HTTP sessions. Increase it only when the ADME schema service and network can tolerate more concurrent requests; reduce it if throttling occurs.

Wide output expands struct-array children up to `WIDE_MAX_CARDINALITY_CAP` positions per parent record. Keep the default unless consumers explicitly need more repeated elements in a single wide table; normalized output is safer for high-cardinality arrays.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| Bronze table not found | Confirm the lakehouse is attached, or set `WORKSPACE_ID`, `LAKEHOUSE_ID`, and `BRONZE_TABLE`. |
| Active-record filter fails | Confirm the bronze table has an `isActive` boolean column, or set `INCLUDE_INACTIVE_RECORDS = True` only if inactive records should be transformed. |
| No records processed | Confirm the selected `KINDS` values match active `kind` values in the bronze table. If only inactive rows exist for a kind, set `INCLUDE_INACTIVE_RECORDS = True` only when those rows should be transformed. |
| ADME schema access fails | Confirm `ADME_ENDPOINT`, `ADME_DATA_PARTITION_ID`, and `ADME_AUTH_METHOD` are set. For `MI`, confirm the runner exposes a managed identity and `ADME_MANAGED_IDENTITY_CLIENT_ID` is set only when using a user-assigned identity. For `SP`, confirm `ADME_TENANT_ID`, `ADME_SP_CLIENT_ID`, `ADME_SP_SECRET_KV_NAME`, and `ADME_SP_SECRET_NAME` are set and the notebook can read the Key Vault secret. |
| Schema load fails | Confirm the runtime can reach the ADME endpoint, MSAL can get a token, and the kind URN exists in the ADME schema service. Review retry settings and schema access details in `silver_run_info`. |
| Schema calls are throttled | Increase retry total or backoff, reduce the number of selected kinds, or keep persisted schema cache enabled. |
| Schema preflight is too slow or throttled | Tune `SCHEMA_FETCH_PARALLELISM`. Lower it for throttling or constrained networks; raise it cautiously for broad runs with many schemas. |
| Private/custom schemas are skipped | Confirm the schema is registered in the ADME data partition and the configured identity is authorized to read it, or use `MISSING_SCHEMA_MODE = "infer"` for best-effort output marked as `schema_mode = "inferred"` in `silver_run_manifest`. |
| Multiple versions collide | Keep `VERSION_STRATEGY = "versioned_tables"` for physical separation, or use `merge` only when one table across versions is intended. |
| Setup checklist fails | Fix the failed checklist item before running with `RUN_PROFILE = "full"`. |
| Full refresh is blocked by existing tables | Review the listed tables and set `ALLOW_OVERWRITE = True` only if replacing them is intended. |
| Upsert merge fails | Fix the Delta merge error and rerun. The notebook does not fall back to overwrite when an existing Delta target fails to merge. |
| Watermark filtering is not active | Confirm `WRITE_MODE = "upsert"`, verify `ingestTime` exists in bronze or override `INCREMENTAL_WATERMARK_COLUMN`, and use `INCREMENTAL_WATERMARK_MODE = "required"` if fallback processing should fail. |
| Watermark upsert refuses to run with limits | Remove `LIMIT` and `KIND_LIMITS`, or set `INCREMENTAL_WATERMARK_MODE = "off"` for bounded test runs that should not advance watermark state. |
| Outputs are not marked committed | Check `silver_run_status` for the latest row for the run. If the final status is `failed` or no `committed` row exists, treat output tables from that run as partial and review `silver_run_info` for the failing kind or metadata write. |
| Data-quality issues are reported | Review `silver_data_quality_issues` and the `quality_status` / `quality_issue_count` fields in run metadata. Findings are non-blocking examples capped by `DATA_QUALITY_MAX_EXAMPLES`; fix source data or merge-key configuration when severity is `error`. |
| Output table shape is too normalized | Use `OUTPUT_MODE = "wide"` to create one wide table per selected kind. |
| Wide output has too many or too few repeated columns | Tune `WIDE_MAX_CARDINALITY_CAP`, or switch to normalized output for array-heavy entities. |
| Output tables are overwritten unexpectedly | Check `RUN_PROFILE`, `WRITE_MODE`, environment overrides, and `ALLOW_OVERWRITE` before running with `RUN_PROFILE = "full"`. |

## Security

Do not report security vulnerabilities through public GitHub issues. For security reporting guidance, see [SECURITY.md](SECURITY.md).

## License

This project is licensed under the [MIT License](LICENSE.md).
