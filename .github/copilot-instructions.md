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

The customer-facing deliverable is `ADME ACZ Silver Layer.ipynb`. It reads ACZ bronze OSDU records from a Fabric/OneLake Delta table, unwraps the Storage record envelope in the bronze `data` payload, resolves kind schemas from the ADME schema service, flattens/reassembles nested JSON, and writes Silver Layer Delta outputs plus run metadata. Normalize envelope `createTime` and `modifyTime` epoch-millisecond values to timestamps when the corresponding bronze columns are timestamp-typed.

`src/adme_acz_silverlayer/` contains local-development helpers for behavior that also exists in the notebook: configuration parsing, table naming, JSON Schema compatibility, Fabric/OneLake boundaries, ADME auth/schema URLs, bronze active-record filtering, runtime safety checks, metadata helpers, and notebook synchronization. The notebook must stay self-contained for Fabric import; customer runtime code must not import `adme_acz_silverlayer`.

The notebook execution sections are a tested contract: Spark runtime configuration, Configuration, Pipeline constants, Helper functions, Core decomposition and reassembly logic, Pipeline functions, Setup checklist, Smoke test bronze access, Run pipeline, and Results summary. Setup and smoke-test sections must remain before pipeline execution.

Outputs support two main shapes: `normalized` creates parent tables plus child tables for arrays, while `wide` creates one reassembled table per kind. Schema versions are either kept as physical `versioned_tables` with `__v1_2_0` suffixes or grouped by family with `merge`. Upsert mode uses merge keys, defaults to `["id", "version"]`, and uses the ACZ `ingestTime` watermark to select source changes. Enabled bridges also reconcile unpruned published references in the selected source kinds against the pinned global target snapshot.

Notebook parent `record_key` is the exact string `id:version`, excluding kind; versions contain only ASCII digits and decoding splits at the final colon without numeric conversion. Children carry the parent reference, never child uniqueness. Keep the canonical `normalization.record_key` synchronized into the notebook, preserve original identity/merge semantics, and exclude this reference from wide payload expansion and child classification. Pinned Bronze preflight rejects invalid identities/versions, reserved-name conflicts and cross-kind exact ID/version conflicts.

Persist notebook scalar bridges with exactly five string columns in this order: `source_record_key`, `target_record_key`, `source_kind`, `status`, `run_id`. Construct target keys from matched exact versions. Incompatible bridge schemas require a clean rebuild and separate consumer/ontology rebind; never retain removed fields through schema alignment. The consolidated diagnostic `build_silver` package API has a distinct output contract.

Schema-declared scalar relationships remain as raw values on parent rows. When `WRITE_RELATIONSHIP_BRIDGES` is enabled, they are published to bridge tables keyed by source kind, relationship path, and declared target type; when disabled, skip bridge planning, resolution, and writes without deleting existing tables. With schema preflight enabled, reuse one narrow cached Bronze identity lookup (`id`, `version`, `kind`, `isActive`) across relationship-bearing groups and release it after the build. Expand scalar references into a narrow row stream and resolve all paths with one join per kind. Partition latest-version ranking by target-type set and ID so overlapping or polymorphic declarations retain their own eligibility rules. Do not add duplicate foreign-key columns to parent outputs; include enabled bridge table names in result manifests and output documentation, and record the enabled state in the run manifest.

## Repository-specific conventions

- When changing notebook logic that has an extracted helper in `src/adme_acz_silverlayer/`, keep the notebook and helper behavior in sync. Tests such as `test_extracted_modules.py`, `test_runtime_boundary_modules.py`, and `test_runtime_metadata_modules.py` compare extracted notebook functions with package helpers.
- Keep one assignment-only customer-settings cell tagged `parameters`, with internal constants outside it. Synchronize the typed parameter contract from `config.py`; resolve present environment values before injected values/defaults without mutating the parameter assignments. Fabric collection inputs use JSON strings.
- OneLake fallback paths share the validated Get Lakehouse layout for the selected workspace/lakehouse IDs. Use execution-identity Fabric tokens; only absent `defaultSchema` in valid metadata means schema-less. Never guess a path after lookup failures.
- Local Spark/Delta test sessions bind their driver and local IP to `127.0.0.1` and set worker Python to the running interpreter. Scope environment patches and cleanup to the test class; an installed but broken runtime fails rather than silently skipping.
- Tests must run independently of Azure accounts and deployed services. Use mocked authentication and stubbed or local fake ADME endpoints; do not add Azure login requirements or live-tenant test paths.
- Keep committed notebooks clean: no code-cell outputs, no execution counts, only markdown/code cells, expected headings in order, and no runtime imports from `adme_acz_silverlayer`. Use `scripts\sync_notebook.py --check --summary` before committing notebook edits.
- Preserve table naming helpers instead of adding ad hoc naming: OSDU kinds map through `kind_to_table_name`, child tables use `{parent}___{full_array_path}`, and versioned tables append `__v<version>`. Array names exceeding 128 characters use deterministic hash suffixes and, when needed, shortened parent prefixes; use `_is_array_child` for ownership and inactive-row cleanup. Relationship bridges use `{parent}__{field}__{target_type}` via `relationship_bridge_table_name`, with deterministic shortening to 128 characters; use `_is_relationship_bridge` with the owning parent to distinguish bridges from array tables, including shortened parent prefixes.
- Active bronze records are the default contract. `INCLUDE_INACTIVE_RECORDS = False` requires an `isActive` column and filters to `isActive == true`; inactive rows in watermark upsert flows are handled as deletes unless inactive records are explicitly included.
- Watermark-based upsert must not run with `LIMIT` or `KIND_LIMITS` unless `INCREMENTAL_WATERMARK_MODE` is `off`; this prevents advancing state after a partial batch.
- `run_silver_build` owns a destination-wide Delta guard and a pinned publication attempt. Keep processing-context resolution in `_processing_contract`; checkpoints are eligible only for matching committed attempts. Persist output ownership before mutation and use the same pinned Bronze input for processing and reference resolution. An incomplete upsert replays its retained snapshot; an authorized unlimited full refresh may supersede it.
- Buffer group watermarks until bridge publication and all required finalization succeed; only committed attempts authorize their progress. Only completed groups contribute bridge publication tables; planned outputs must not erase a failed group's existing bridge. Bridge upserts decode source record keys transiently to match effective identity merge keys: ID-only replacement removes earlier source versions; ID/version replacement preserves other versions. Pass the bridge output role explicitly to shared writers; ordinary payload columns and custom merge keys may also be named `source_id`, `source_version`, or `source_kind`. Disabling bridges also prevents inactive-source cleanup from deleting their historical rows.
- Full refresh and merge paths are intentionally strict: full refresh must respect `ALLOW_OVERWRITE`, and failed Delta merge paths must not silently fall back to overwrite.
- Reconcile current and historical child/bridge tables, including empty desired frames. Full refresh replaces only selected `osdu_kind`/`source_kind` rows through `replaceWhere`, preserving unselected kinds and shared columns. Empty refresh requires readable ownership and a resolved schema; planned-only ownership permits repeated empty refreshes, but physically present children/bridges require their parent. Disabled bridges stay untouched.
- Use retry-enabled ADME schema HTTP helpers and configured endpoint/partition values. The notebook does not fall back to the public OSDU data-definitions repository.
- Keep metadata surfaces consistent when adding pipeline outputs or states: `silver_run_info`, `silver_run_manifest`, `silver_run_status`, `silver_data_quality_issues`, `silver_schema_cache`, `silver_incremental_state`, `silver_output_documentation`, `silver_publication_attempts`, `silver_output_ownership`, and `silver_writer_guard`.
- `samples/` files are placeholder onboarding assets. Do not commit tenant IDs, concrete GUIDs, secrets, passwords, or customer-specific values; sample config JSON files should keep `description`, `settings`, and `RUN_PROFILE`.
