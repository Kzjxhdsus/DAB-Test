# NRL Bronze Flattening Framework (Variant)

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [How It Works](#how-it-works)
4. [Config Reference](#config-reference)
5. [Runner Operating Modes](#runner-operating-modes)
6. [Write Modes and Incremental Logic](#write-modes-and-incremental-logic)
7. [Output Tables and Lineage Columns](#output-tables-and-lineage-columns)
8. [Audit and Error Logging](#audit-and-error-logging)
9. [post_run_fn Pattern](#post_run_fn-pattern)
10. [Adding a New Config](#adding-a-new-config)
11. [Troubleshooting](#troubleshooting)

---

## Overview

The variant framework flattens nested JSON/XML bronze Delta tables into a set of flat, query-ready Delta tables in the `bronze_stats_flatten` schema. It is the production pipeline for **Databricks Job Compute** clusters.

There are two framework files:

| File | Purpose | Environment |
|------|---------|-------------|
| `framework_variant.py` | Core flattening engine | Job Compute only |
| `runner_variant.py` | Entry-point notebook; loads configs, drives the pipeline | Job Compute only |

> **Not compatible with Databricks Serverless.** Serverless clusters use `flatten_framework.py` + `runner.py` instead. The config format is identical across both — the runner and framework are the only things that differ.

---

## Architecture

```
Databricks Workflow
│
├── Task 1 — Initialize_Log_Tables
│     Creates audit + error log tables once before the ForEach fans out.
│
├── Task 2 — Discover Configs (task_2_discover_configs)
│     Queries the audit log to find which (config, competition, season)
│     combinations need processing, publishes a JSON list for the ForEach.
│
└── Task 3 — ForEach → runner_variant.py
      For each item in the list:
        1. Read the config module from configs/nrl_<name>_config.py
        2. Build a bronze_filter (source scope + watermark window)
        3. Call run_multi_table_pipeline()  ← framework_variant.py
        4. Call post_run_fn() if present    ← defined inside the config
        5. Write an audit log entry
```

### Why "Variant"?

The bronze tables store the JSON/XML payload in a Databricks native **VARIANT** column. The variant framework uses VARIANT-native SQL functions (`try_variant_get`, `schema_of_variant_agg`, `variant_explode`) instead of UDFs, `from_json`, or Python-side string parsing. This makes it faster and avoids the overhead of serialising data out of JVM memory.

| Non-variant approach | Variant approach |
|---------------------|-----------------|
| `variant_to_str` UDF to cast VARIANT → STRING | VARIANT column used directly |
| `_json_split_array` UDF to explode arrays | `cast(... as array<variant>)` + `explode()` |
| `from_json` with manually defined schema | `schema_of_variant_agg()` infers schema automatically |
| Python sampling loop over collected rows | Single Spark aggregation over the full column |

---

## How It Works

### Bronze table structure

Every bronze table has the same base columns:

| Column | Type | Description |
|--------|------|-------------|
| `ingested_from` | STRING | Identifies the source API call, e.g. `match_snapshot_111_2026_20261110110` |
| `ingested_at` | TIMESTAMP | When the row was ingested into bronze |
| `l0payload` | VARIANT | Root-level scalar attributes (present for some APIs, NULL for others) |
| `l1element` | STRING | Names the section of the API response this row holds, e.g. `gameInfo`, `teams` |
| `payload` | VARIANT | The actual nested JSON/XML payload for this element |

### Step-by-step pipeline (`run_multi_table_pipeline`)

The pipeline runs once per `l1element` declared in the config's `elements` map.

```
For each l1element:

  Step 1 — Read bronze
           Apply bronze_filter (source scope + watermark window) to the
           bronze Delta table.

  Step 2 — Filter to this l1element
           bronze_df.filter(l1element == 'teams')

  Step 3 — Schema inference
           schema_of_variant_agg(payload) → DDL string
           e.g. OBJECT<teamId: STRING, teamName: STRING, players: ARRAY<OBJECT<...>>>
           Parsed into a PySpark StructType.

  Step 4 — L0 detection
           If l0payload is non-null, the root scalar fields (game context, etc.)
           come from l0payload, not from payload. Both are processed separately.

  Step 5 — Build schema tree
           Walk the StructType recursively. Every ARRAY<OBJECT<...>> field
           becomes a child node. Each node records:
             • alias          — field name in snake_case
             • target_table   — full Delta table name to write
             • dedup_keys     — columns used for deduplication
             • write_mode     — overwrite or append
             • arrays         — list of child nodes (recursion)
             • schema         — StructType of one element at this level

  Step 6 — Root DataFrame preparation
           ARRAY payload   → cast(payload as array<variant>) + explode()
                             each element becomes one row
           OBJECT payload  → payload assigned directly as a VARIANT column

  Step 7 — Recursive tree traversal (_process_node)
           At each node:
             a. Extract scalar fields using try_variant_get(variant_col, '$.field', 'TYPE')
             b. Write ancestor columns + scalar columns to the node's target_table
             c. For each child array: navigate with try_variant_get → VARIANT,
                cast to array<variant>, explode, recurse into the child node
```

### L0 mode vs Standard mode

| Mode | When used | l0payload | payload |
|------|-----------|-----------|---------|
| **Standard** | l0payload is NULL for all rows | — | Contains the full data array |
| **L0** | l0payload has values | Root scalar context (game ID, competition, etc.) | Contains child arrays only |

In L0 mode, `l0payload` scalar fields are extracted first and passed down as **ancestor columns** into every child table, so every row in every output table carries the game/competition context.

### force_array_fields and coerce_to_array

XML-to-JSON conversion sometimes produces a single-element object where the schema should be an array (e.g. one player in a squad produces `{"player": {...}}` instead of `{"player": [...]}`). Two config options handle this:

- **`force_array_fields`** — tells `_discover_arrays` to treat a field the schema inferred as `OBJECT` as a one-element array
- **`coerce_to_array`** — set automatically for VARIANT-typed `force_array_fields`; splits rows into array-shaped and object-shaped DataFrames and unions them to avoid `INVALID_VARIANT_CAST`

---

## Config Reference

Each API has one config file at `configs/nrl_<name>_config.py`. The file exports a single dict named `<NAME>_CONFIG`.

### Full config options

```python
MY_CONFIG = {

    # ── Required ──────────────────────────────────────────────────────────────

    "bronze_table": "nrl_datalakehouse_qa.bronze_stats.my_table",
    # Fully qualified name of the source Delta table.

    "target_prefix": "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_my_table",
    # All output table names are derived as f"{target_prefix}_{alias}".
    # The root table is written as f"{target_prefix}_{root_alias}".

    "elements": {
        "teams":     "teams",      # l1element value → root alias (used in table name)
        "gameInfo":  "game_info",  # camelCase l1element → snake_case alias
    },
    # Maps each l1element name to the snake_case alias used for table naming.

    # ── Write behaviour ────────────────────────────────────────────────────────

    "write_mode": "append",
    # "append"    — add rows to existing tables (used for incremental loads)
    # "overwrite" — replace tables entirely (first-time / full reload)
    # Note: runner_variant always forces "append" regardless of this value.

    # ── Filtering ──────────────────────────────────────────────────────────────

    "source_filter_template": "ingested_from LIKE 'my_table_{competition_id}_{season}%'",
    # SQL WHERE clause template. Placeholders {competition_id} and {season} are
    # filled from the ForEach input JSON at runtime.
    # If the API has no season (competition-level only), omit {season}:
    #   "ingested_from LIKE 'my_table_{competition_id}%'"

    # ── Schema control ────────────────────────────────────────────────────────

    "exclude_fields": {
        "teamStats",     # prevent the framework from including or exploding this field
        "playerStats",   # useful when post_run_fn handles these nested structures
    },
    # Fields listed here are skipped entirely — not written as columns and not
    # exploded as child tables. Use when post_run_fn owns the handling of a
    # nested structure, or when a field would create a table name collision.

    "flatten_structs": {
        "score": "score_",    # flatten OBJECT fields into prefixed columns
    },
    # Maps a struct field name to a column prefix.
    # score.tries → score_tries, score.conversions → score_conversions, etc.
    # Only one level deep.

    "force_array_fields": {
        "player",   # treat this OBJECT field as a single-element array
    },
    # Use when XML single-element conversion produces {"player": {...}} instead of
    # {"player": [{...}]}.  Forces _discover_arrays to treat it as an array.

    "exclude_root_aliases": {
        "game_info",   # suppress writing the root table for this element
    },
    # Aliases listed here will have their root table write suppressed.
    # Child tables are still written. Useful when two l1elements produce children
    # that should share a parent table you don't want duplicated.

    # ── Deduplication ─────────────────────────────────────────────────────────

    "dedup_keys_map": {
        "teams":     ["ingested_from", "team_id"],
        "game_info": ["ingested_from"],
    },
    # Per-alias dedup keys. When a table is written, it is first deduplicated
    # by keeping the most-recently-ingested row per unique key combination.
    # Omit an alias to skip deduplication for that table.

    # ── Type control ─────────────────────────────────────────────────────────

    "type_overrides": {
        "player_id": "BIGINT",   # override the inferred type for a specific field
    },
    # Applied after schema_of_variant_agg inference. Use sparingly — the
    # framework infers types correctly in most cases.

    # ── Static columns ────────────────────────────────────────────────────────

    "static_columns": {
        "competition": "NRL",   # add a fixed-value column to every output row
    },
    # Optional. Useful for adding context not present in the payload.

    # ── Sample size (unused in variant framework) ─────────────────────────────

    "sample_size": 200,
    # Kept for config-format compatibility with flatten_framework.py.
    # framework_variant.py uses schema_of_variant_agg over the full column
    # and ignores this value.

    # ── Post-processing ────────────────────────────────────────────────────────

    "post_run_fn": _my_post_run_fn,
    # Optional. A Python function called after run_multi_table_pipeline completes.
    # Signature: fn(config: dict, spark: SparkSession) → None
    # Used for deeply nested structures where the framework's auto-discovery
    # is not sufficient. See the post_run_fn pattern section below.
}
```

### ingested_from parsing

The `ingested_from` string identifies the source API call. Its format mirrors the bronze table name followed by the API parameters:

```
<table_name>_<competition_id>_<season>_<round>_<game_id>
```

Since the table name itself uses underscores, **fixed-index parsing** (`parts[1]`, `parts[2]`, etc.) will be wrong. The correct approach is to count the words in the table name prefix:

| Bronze table name | Prefix words | competition_id | season_id | round / game_id |
|-------------------|-------------|---------------|-----------|----------------|
| `match_snapshot` | 2 | `parts[2]` | `parts[3]` | `parts[-1]` |
| `form_guide_for_match` | 4 | `parts[4]` | `parts[5]` | `parts[-1]` |
| `team_leaderboards` | 2 | `parts[2]` | `parts[3]` | `parts[4]` |
| `team_leaderboards_average` | 3 | `parts[3]` | `parts[4]` | `parts[5]` |
| `top_three_leaderboard` | 3 | `parts[3]` | — | — |

For game-level APIs (where `game_id` is a long numeric ID like `20261110110`), always use `parts[-1]` — the game ID is always the last segment regardless of how many round/sub-round numbers appear in the middle.

---

## Runner Operating Modes

`runner_variant.py` is a Databricks notebook that acts as the entry-point for the pipeline. It supports two modes detected automatically at runtime.

### ForEach mode

Used when the notebook is launched by a Databricks Workflow **ForEach** task. The ForEach task injects a JSON payload via the `foreach_input` widget:

```json
{
    "config_name":    "matchsnapshotdata",
    "competition_id": "111",
    "season":         "2026",
    "audit_key":      "matchsnapshotdata_111_2026",
    "src_watermark":  "2026-04-10T08:00:00",
    "tgt_watermark":  "2026-04-09T06:00:00"
}
```

- `src_watermark` — `MAX(ingested_at)` in bronze at the time Task 2 ran (upper bound)
- `tgt_watermark` — `MAX(ingested_at)` from the last successful audit log entry (lower bound); `null` on the first run

### Direct mode

Used when running the notebook manually or as a standalone job with plain widget parameters:

| Widget | Value |
|--------|-------|
| `config_name` | e.g. `matchsnapshotdata`, or blank / `all` for every config |
| `competition_id` | e.g. `111` |
| `season` | e.g. `2026` |

No watermark filtering is applied. All matching bronze rows are processed.

### Config resolution

The runner loads configs dynamically by name:

```
config_name = "matchsnapshotdata"
  → module  : configs.nrl_matchsnapshotdata_config
  → object  : MATCHSNAPSHOTDATA_CONFIG
```

The naming convention is strict: `configs/nrl_<name>_config.py` with `<NAME>_CONFIG` as the exported dict.

---

## Write Modes and Incremental Logic

### How write_mode is determined

`runner_variant.py` currently always sets `write_mode = "append"` regardless of the config value. This means:

- **All runs are append-only.** Existing rows in target tables are never deleted.
- The config-level `"write_mode": "append"` is consistent with this.

> A future enhancement will set `write_mode = "overwrite"` on the first run (when `tgt_watermark is None`) to replace any stale tables from previous test runs.

### bronze_filter construction

The runner builds a two-part filter and stores it in `config["bronze_filter"]`:

```
Part 1 — source scope (from source_filter_template)
         ingested_from LIKE 'match_snapshot_111_2026%'

Part 2 — watermark window (incremental runs only, when tgt_watermark is not None)
         ingested_at > '2026-04-09T06:00:00' AND ingested_at <= '2026-04-10T08:00:00'

Combined:
         ingested_from LIKE 'match_snapshot_111_2026%'
         AND ingested_at > '2026-04-09T06:00:00'
         AND ingested_at <= '2026-04-10T08:00:00'
```

Part 1 ensures only the right competition/season is read.
Part 2 ensures only new bronze rows (since the last successful run) are processed.
On the first run, Part 2 is omitted — the full history is processed.

### Audit key

In ForEach mode, the audit log entry uses the composite key from the ForEach input (e.g. `matchsnapshotdata_111_2026`). Task 2 looks this key up on the next run to determine the target watermark. In Direct mode, the plain config name is used.

---

## Output Tables and Lineage Columns

Every table written by the framework has four lineage columns appended automatically by `write_table()`:

| Column | Value | Example |
|--------|-------|---------|
| `source_schema` | Schema of the bronze source table | `bronze_stats` |
| `source_table_name` | Name of the bronze source table | `match_snapshot` |
| `target_schema` | Schema of the output table | `bronze_stats_flatten` |
| `target_table_name` | Name of the output table (short name) | `nrl_match_snapshot_team` |

These are in addition to `ingested_at` and `ingested_from` which are passed through from the bronze table as ancestor columns.

### Table naming

Given `target_prefix = "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot"` and `elements = {"teams": "teams", "gameInfo": "game_info"}`, the framework produces:

```
nrl_match_snapshot_teams       ← root table for the 'teams' element
nrl_match_snapshot_team        ← exploded child array 'team' under 'teams'
nrl_match_snapshot_game_info   ← root table for the 'gameInfo' element
```

**Table name collision rule:** If two elements both produce a child array with the same name (e.g. both `interchangeFlow` and `disciplineFlow` produce a `team` child), they will write to the same table. If that table was written by the framework with TIMESTAMP `ingested_at` and a `post_run_fn` then tries to append with STRING `ingested_at`, the job will fail with `DELTA_FAILED_TO_MERGE_FIELDS`. The fix is to add the colliding field name to `exclude_fields`.

---

## Audit and Error Logging

### Audit log

Written after every **successful** config run.

```
nrl_datalakehouse_qa.bronze_stats.nrl_pipeline_audit_log

run_id       STRING     — UUID shared across all configs in one notebook execution
flattened_at TIMESTAMP  — UTC timestamp of the audit write
config_name  STRING     — audit_key (composite) in ForEach mode, plain name in Direct mode
source_table STRING     — bronze table name
ingested_at  TIMESTAMP  — MAX(ingested_at) from the bronze table scoped to bronze_filter
```

Task 2 reads `MAX(ingested_at)` from this table per `config_name` to compute the target watermark for the next run.

### Error log

Written whenever a config fails.

```
nrl_datalakehouse_qa.bronze_stats.nrl_pipeline_error_log

run_id          STRING     — same UUID as the audit log
run_timestamp   TIMESTAMP  — UTC timestamp of the failure
pipeline_step   STRING     — config name that failed
error_message   STRING     — Python exception message
error_traceback STRING     — full stack trace
```

Failed configs do not stop the job — remaining configs in the run still execute. After all configs complete, if any failed, the notebook raises a `RuntimeError` listing the failed names so the Databricks Workflow marks the task as failed.

Both log tables are created once by **Task 1 (Initialize_Log_Tables)** before the ForEach fans out, preventing `MetadataChangedException` from concurrent DDL when multiple iterations start at the same time.

---

## post_run_fn Pattern

Some APIs have deeply nested payloads (3+ levels, or branching structures) that the framework's automatic tree traversal cannot handle cleanly. For these, a `post_run_fn` is defined directly inside the config file and registered in the config dict.

```python
def _create_my_tables(config, spark):
    bronze        = config["bronze_table"]
    target_prefix = config["target_prefix"]
    write_mode    = config.get("write_mode", "overwrite")

    bronze_df = spark.table(bronze)
    bf = config.get("bronze_filter")
    if bf:
        bronze_df = bronze_df.filter(bf)

    rows = (
        bronze_df
        .select("l1element", "ingested_from", "ingested_at",
                F.col("l0payload").cast("string").alias("l0"),
                F.col("payload").cast("string").alias("p"))
        .collect()
    )

    records = []
    for row in rows:
        payload = json.loads(row.p) if row.p else {}
        # ... parse nested structures, build flat record dicts ...
        records.append({
            "field_a": ...,
            "field_b": ...,
            "ingested_from": row.ingested_from,
            "ingested_at":   str(row.ingested_at),
        })

    # Write all-STRING schema table
    _write(records, f"{target_prefix}_my_suffix", spark, write_mode)

MY_CONFIG = {
    ...
    "post_run_fn": _create_my_tables,
}
```

### post_run_fn conventions

- **All values are written as STRING.** The `_write` helper inside each post_run_fn builds an all-STRING `StructType` and casts every value with `str()`.
- **`ingested_at` is stored as STRING** (via `str(row.ingested_at)`). This is different from the framework, which stores it as TIMESTAMP.
- **`bronze_filter` must be applied.** The post_run_fn receives the same `config` dict (including `bronze_filter` set by the runner), and must apply it when reading the bronze table.
- **Do not write to table names also written by the framework.** If a field name in `exclude_fields` is a child array the framework would otherwise auto-discover (e.g. `"team"` under `interchangeFlow`), the framework will skip it — but if `"team"` is NOT in `exclude_fields`, the framework writes the table first with TIMESTAMP schema, and the post_run_fn then fails trying to append with STRING schema. Always add such field names to `exclude_fields`.

---

## Adding a New Config

1. **Create the config file** at `configs/nrl_<name>_config.py`

2. **Set the bronze table and target prefix**
   ```python
   "bronze_table":  "nrl_datalakehouse_qa.bronze_stats.<source_table_name>",
   "target_prefix": "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_<table_name>",
   ```

3. **Set the source_filter_template**
   Count the underscores in the bronze table name to get the right `parts[N]` indices:
   ```python
   "source_filter_template": "ingested_from LIKE '<bronze_table>_{competition_id}_{season}%'",
   ```

4. **Declare elements**
   Map each `l1element` value to a snake_case alias:
   ```python
   "elements": {"teams": "teams", "gameInfo": "game_info"},
   ```

5. **Set write_mode to append**
   ```python
   "write_mode": "append",
   ```

6. **Add exclude_fields** for any deeply nested structures the post_run_fn will own, and for any field name that would collide with a post_run_fn output table.

7. **Add a post_run_fn if needed** for 3+ level nesting or branching structures.

8. **Register the config name** in `runner_variant.py` inside `_all_config_names()`:
   ```python
   def _all_config_names():
       return [
           ...,
           "my_new_config",   # add here
       ]
   ```

9. **Test with Direct mode** first:
   - Set `config_name` widget to your config name
   - Set `competition_id` and `season`
   - Run the runner notebook

---

## Troubleshooting

### `DELTA_FAILED_TO_MERGE_FIELDS` on `ingested_at`

**Cause:** A table was written by the framework (TIMESTAMP `ingested_at`) and then the post_run_fn tried to append to the same table with STRING `ingested_at`.

**Fix:**
1. Add the colliding field name to `exclude_fields` in the config so the framework no longer writes that table.
2. Drop the existing table that the framework wrote (it has the wrong schema).
3. Re-run — the post_run_fn will recreate it fresh with all-STRING schema.

### `ModuleNotFoundError: No config file found for 'xyz'`

The config name passed to the runner does not match any file in `configs/`. Check:
- File exists: `configs/nrl_xyz_config.py`
- Config dict is named: `XYZ_CONFIG`
- Name is registered in `_all_config_names()` if running in "all" mode

### `ValueError: Config module has no object named 'XYZ_CONFIG'`

The exported dict in the config file is named differently from what the runner expects. The runner looks for `<NAME.upper()>_CONFIG`. Ensure the dict name matches exactly.

### `[pre-flight] Bronze table does not exist`

The `bronze_table` value in the config points to a table that doesn't exist in the catalog. Check the catalog, schema, and table name spelling.

### `WARNING: no rows for l1element='xyz' — skipping`

The bronze table has no rows with that `l1element` value after the bronze_filter is applied. Check:
- The `source_filter_template` is using the correct `ingested_from` prefix (matching the actual table name, not the old camelCase API name)
- Data has been ingested for the competition/season being requested
- The `ingested_from` split indices are correct for the number of words in the bronze table name

### Framework version mismatch

The runner prints `[framework_variant] version: N` at startup. If the version doesn't match the latest file on disk, the cluster is using a cached import. The runner calls `importlib.reload(_fv)` to force a reload — if the version is still wrong, detach and reattach the cluster.
