# Changelog

Generated from the repository commit history. Dates are grouped by commit date in reverse chronological order.

## 2026-07-03 (2 commits)

- Refactored the notebook development foundation with local package metadata, notebook sync validation, extracted pure runtime and metadata helpers, and expanded tests while keeping the Fabric notebook self-contained (`0f3cdfe`).
- Extracted runtime boundary helper modules for Fabric/OneLake access, bronze filtering, ADME schema/auth boundaries, and notebook parity tests; aligned notebook regression expectations with committed defaults (`a176f42`).

## 2026-07-02 (1 commit)

- Added richer kind filtering and configuration options, including `EXCLUDED_KINDS`, empty-list defaults in samples, `ingestTime` as the scheduled incremental watermark, optional selector handling, excluded-kind tests, and pruning changed rows before schema access in incremental runs (`04a2c0b`).

## 2026-07-01 (3 commits)

- Sanitized Silver Layer Delta column names and added related coverage (`1ceff19`).
- Ignored temporary workspace folders (`a4f411a`).
- Refactored schema handling to deduplicate case-insensitive struct fields across schema conversion and merge paths, with helper functions and unit tests for the deduplication behavior (`0cb858b`).

## 2026-06-29 (7 commits)

- Expanded ADME schema service integration documentation and tests, including authentication guidance, incremental processing controls, watermark settings, write modes, retry status parsing, monitoring, and performance guidance (`55ebb2f`).
- Refactored the README for clearer notebook usage instructions, output descriptions, and configuration guidance (`e526594`).
- Added inactive record handling through `INCLUDE_INACTIVE_RECORDS`, active-record filtering logic, README guidance, and tests (`eb44ab3`).
- Added reusable configuration and sample files for dry runs, interactive service principal runs, scheduled managed identity runs, Fabric pipeline parameters, and synthetic bronze records; introduced package helper modules for configuration, naming, and schema compatibility with tests (`db06cf8`).
- Bumped the notebook version to `0.3.0`, added notebook cell IDs, and normalized notebook metadata (`08b2a2e`).
- Added bronze record normalization for ACZ payload and storage record envelopes, wired it into Spark table reads, and updated README/tests (`96e4770`).
- Bumped the notebook version to `0.4.0`, changed kind-to-table naming to include authority and source, and updated README/tests for the new naming convention (`08a9c51`).

## 2026-06-28 (1 commit)

- Refreshed the Silver Layer notebook, README, and tests with explicit Spark runtime defaults, enhanced logging, schema caching, persistent cache helpers, Fabric/OneLake path fallbacks, upsert/merge helpers, web-backed OSDU schema registry loading, naming helpers, decomposition/reassembly helpers, schema parsing, and type mapping (`ce8ded1`).

## 2026-06-25 (2 commits)

- Documented and tested version strategy controls, missing schema handling, child table creation behavior, version strategy helpers, and schema URL candidate generation (`83e9659`).
- Added incremental merge key support through `MERGE_KEY_COLUMNS`, documented `id`/`version` merge keys, and added tests for merge key parsing, merge conditions, and performance resilience controls (`931bb53`).

## 2026-06-24 (10 commits)

- Added Microsoft security policy and repository licensing/project setup material, including `SECURITY.MD`, MIT license content, and initial planning artifacts (`a3e1263`, `8fd63a3`, `ac47b45`, `81a67ee`).
- Added the initial ADME ACZ Silver Layer README and notebook documentation covering usage, architecture, and configuration (`9e1a2be`).
- Simplified the notebook workflow by adding `OUTPUT_MODE`, preserving `ADME_REASSEMBLE` as a compatibility alias, removing hard-coded Fabric workspace/lakehouse fallbacks, stripping attachment-specific metadata, moving bronze-access smoke tests earlier, replacing driver-collected incremental child writes with DataFrame-based changed IDs, adding in-memory schema document caching, and adding notebook simplification tests (`338d3f5`).
- Added explicit tenant configuration, `RUN_PROFILE = "dry_run"`, setup checklist guidance, persisted schema cache support, `silver_run_manifest`, README operational updates, and repeatability tests (`f5bb0fe`).
- Added overwrite safety with `ALLOW_OVERWRITE`, `NOTEBOOK_VERSION = "0.2.0"`, deterministic `config_hash`, expanded run manifest metadata, table-name validation, README safety guidance, and tests (`86560c5`).
- Added per-kind row limits through `KIND_LIMITS` / `ADME_KIND_LIMITS`, generated output documentation in `silver_output_documentation`, dry-run/full-run wiring, README guidance, and tests (`fee6692`).
- Implemented wildcard and all-kinds support (`0336f88`).

## 2026-05-26 (2 commits)

- Created the initial repository contents (`de7c0aa`).
- Added Microsoft mandatory repository file requirements (`cf4ee7c`).
