# Copilot instructions for this repository

## Commands

- Validate notebook hygiene and required section order: `python scripts\sync_notebook.py --check --summary`
- Normalize the notebook by removing execution artifacts: `python scripts\sync_notebook.py`
- Run the full unit test suite: `python -m unittest discover -s tests -q`
- Run one test file: `python -m unittest discover -s tests -p test_notebook_sync.py -q`
- Run one test method: `python tests\test_notebook_sync.py NotebookSyncTests.test_committed_notebook_is_clean_and_self_contained -q`
- Install the package locally when needed: `python -m pip install -e .`
- Install optional Spark dependency for local Spark work: `python -m pip install -e .[spark]`

The tests are `unittest`-based. Because `tests` is not a Python package, direct dotted selectors like `python -m unittest tests.test_notebook_sync...` do not work unless the layout changes.

## MCP servers

This project is Azure/Fabric/ADME-focused. When MCP tools are available, use Azure MCP for Azure resource context and Microsoft Learn MCP for current documentation on Azure Data Manager for Energy, Fabric/OneLake, Azure auth, and PySpark/Delta behavior. Azure MCP local CLI setup uses `npx -y @azure/mcp@latest server start` and relies on Azure Identity credentials such as `az login`.

## High-level architecture

The customer-facing deliverable is `ADME ACZ Silver Layer.ipynb`. It reads ACZ bronze OSDU records from a Fabric/OneLake Delta table, unwraps the Storage record envelope in the bronze `data` payload, resolves kind schemas from the ADME schema service, flattens/reassembles nested JSON, and writes Silver Layer Delta outputs plus run metadata.

`src/adme_acz_silverlayer/` contains local-development helpers for behavior that also exists in the notebook: configuration parsing, table naming, JSON Schema compatibility, Fabric/OneLake boundaries, ADME auth/schema URLs, bronze active-record filtering, runtime safety checks, metadata helpers, and notebook synchronization. The notebook must stay self-contained for Fabric import; customer runtime code must not import `adme_acz_silverlayer`.

The notebook execution sections are a tested contract: Spark runtime configuration, Configuration, Pipeline constants, Helper functions, Core decomposition and reassembly logic, Pipeline functions, Setup checklist, Smoke test bronze access, Run pipeline, and Results summary. Setup and smoke-test sections must remain before pipeline execution.

Outputs support two main shapes: `normalized` creates parent tables plus child tables for arrays, while `wide` creates one reassembled table per kind. Schema versions are either kept as physical `versioned_tables` with `__v1_2_0` suffixes or grouped by family with `merge`. Upsert mode uses merge keys, defaults to `["id", "version"]`, and can use the ACZ `ingestTime` watermark to prune changed kinds before schema access.

## Repository-specific conventions

- When changing notebook logic that has an extracted helper in `src/adme_acz_silverlayer/`, keep the notebook and helper behavior in sync. Tests such as `test_extracted_modules.py`, `test_runtime_boundary_modules.py`, and `test_runtime_metadata_modules.py` compare extracted notebook functions with package helpers.
- Keep committed notebooks clean: no code-cell outputs, no execution counts, only markdown/code cells, expected headings in order, and no runtime imports from `adme_acz_silverlayer`. Use `scripts\sync_notebook.py --check --summary` before committing notebook edits.
- Preserve table naming helpers instead of adding ad hoc naming: OSDU kinds map through `kind_to_table_name`, child tables use `{parent}___{full_array_path}`, and versioned tables append `__v<version>`.
- Active bronze records are the default contract. `INCLUDE_INACTIVE_RECORDS = False` requires an `isActive` column and filters to `isActive == true`; inactive rows in watermark upsert flows are handled as deletes unless inactive records are explicitly included.
- Watermark-based upsert must not run with `LIMIT` or `KIND_LIMITS` unless `INCREMENTAL_WATERMARK_MODE` is `off`; this prevents advancing state after a partial batch.
- Full refresh and merge paths are intentionally strict: full refresh must respect `ALLOW_OVERWRITE`, and failed Delta merge paths must not silently fall back to overwrite.
- Use retry-enabled ADME schema HTTP helpers and configured endpoint/partition values. The notebook does not fall back to the public OSDU data-definitions repository.
- Keep metadata surfaces consistent when adding pipeline outputs or states: `silver_run_info`, `silver_run_manifest`, `silver_run_status`, `silver_data_quality_issues`, `silver_schema_cache`, `silver_incremental_state`, and `silver_output_documentation`.
- `samples/` files are placeholder onboarding assets. Do not commit tenant IDs, concrete GUIDs, secrets, passwords, or customer-specific values; sample config JSON files should keep `description`, `settings`, and `RUN_PROFILE`.
