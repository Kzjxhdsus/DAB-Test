# Databricks notebook source
"""
runner_variant.py  (Task 3 — VARIANT)
--------------------------------------
Entry-point for all NRL Bronze → Silver flattening jobs using the
VARIANT-native pipeline (framework_variant.py).

For use on Databricks Job Compute only.
For Databricks Serverless use runner.py + flatten_framework.py instead.

Two operating modes
-------------------
ForEach mode  — launched by the Databricks Workflow ForEach task.
                {{input}} is a JSON dict published by task_2_discover_configs:
                {
                    "config_name":    "all_squads",
                    "competition_id": "111",
                    "season":         "2025",
                    "audit_key":      "all_squads_111_2025",
                    "src_watermark":  "2026-04-10T08:00:00",
                    "tgt_watermark":  "2026-04-09T06:00:00"   ← null on first run
                }
                ForEach passes this JSON as the foreach_input widget value.

Direct mode   — notebook run or one-off job with plain parameters:
                config_name    = "all_squads"   (or blank/"all" for everything)
                competition_id = "111"
                season         = "2025"
                No watermark filtering — behaves like the original runner.

Write behaviour
---------------
Bronze is append-only — rows are always added, never overwritten.
"""

import importlib
import json
import string as _string
import uuid   as _uuid

import framework_variant as _fv
importlib.reload(_fv)
from framework_variant import (
    run_multi_table_pipeline,
    log_pipeline_error,
    log_pipeline_audit,
)

# COMMAND ----------

# ------------------------------------------------------------------
# Log table names
# Workflow mode : read from task values published by Task 1.
# Direct mode   : fall back to hardcoded names so the notebook still
#                 works when run outside the full workflow.
# ------------------------------------------------------------------
try:
    ERROR_LOG_TABLE = dbutils.jobs.taskValues.get(          # noqa: F821
        taskKey = "Initialize_Log_Tables",
        key     = "error_log_table",
    )
    AUDIT_LOG_TABLE = dbutils.jobs.taskValues.get(          # noqa: F821
        taskKey = "Initialize_Log_Tables",
        key     = "audit_log_table",
    )
    print("[runner_variant] Log table names read from Task 1 task values")
except Exception:
    ERROR_LOG_TABLE = "sandbox.ashish.nrl_pipeline_error_log"
    AUDIT_LOG_TABLE = "sandbox.ashish.nrl_pipeline_audit_log"
    print("[runner_variant] Log table names from fallback (direct / interactive run)")

print(f"[runner_variant] ERROR_LOG_TABLE = {ERROR_LOG_TABLE}")
print(f"[runner_variant] AUDIT_LOG_TABLE = {AUDIT_LOG_TABLE}")

# COMMAND ----------

# ------------------------------------------------------------------
# Read the foreach_input widget.
# ForEach mode : contains a JSON string from task_2_discover_configs.
# Direct mode  : plain string, e.g. "all_squads" or "".
#
# Do NOT call dbutils.widgets.text() — on All-Purpose clusters it
# resets the widget to its default value (""), overwriting the
# {{input}} value injected by the ForEach task.
# ------------------------------------------------------------------
_raw_input     = ""
season         = ""
competition_id = ""

try:
    # foreach_input is set by the ForEach child task as foreach_input = {{input}}
    # Using a dedicated name avoids conflict with the job-level config_name
    # parameter which Task 2 uses to filter configs but which would otherwise
    # overwrite the ForEach JSON dict here.
    _raw_input = dbutils.widgets.get("foreach_input").strip()       # noqa: F821
except Exception:
    pass

if not _raw_input:
    # Fallback for direct runs (no ForEach) — read plain config_name widget
    try:
        _raw_input     = dbutils.widgets.get("config_name").strip()     # noqa: F821
        season         = dbutils.widgets.get("season").strip()          # noqa: F821
        competition_id = dbutils.widgets.get("competition_id").strip()  # noqa: F821
    except Exception:
        pass

# COMMAND ----------

# ------------------------------------------------------------------
# Detect operating mode by attempting to JSON-parse the raw input.
# ------------------------------------------------------------------
is_foreach    = False
config_name   = _raw_input
audit_key     = None
src_watermark = None
tgt_watermark = None

try:
    _parsed       = json.loads(_raw_input)
    # Confirmed JSON — ForEach mode
    config_name    = _parsed["config_name"]
    competition_id = _parsed.get("competition_id", "") or competition_id
    season         = _parsed.get("season",         "") or season
    audit_key      = _parsed.get("audit_key",      config_name)
    src_watermark  = _parsed.get("src_watermark")
    tgt_watermark  = _parsed.get("tgt_watermark")
    is_foreach     = True
    print(f"[runner_variant] Mode: ForEach")
    print(f"[runner_variant] audit_key     = {audit_key}")
    print(f"[runner_variant] src_watermark = {src_watermark}")
    print(f"[runner_variant] tgt_watermark = {tgt_watermark}")
except (json.JSONDecodeError, TypeError, KeyError):
    # Plain string — direct / interactive run, no watermark filtering
    print(f"[runner_variant] Mode: Direct")

# Build run_params (used by source_filter_template rendering)
run_params = {}
if competition_id: run_params["competition_id"] = competition_id
if season:         run_params["season"]         = season

print(f"[runner_variant] config_name    = '{config_name}'")
print(f"[runner_variant] run_params     = {run_params if run_params else '(none)'}")

# COMMAND ----------

# ------------------------------------------------------------------
# Resolve list of configs to run.
# ForEach mode always delivers exactly one config per iteration.
# Direct mode can run one or all.
# ------------------------------------------------------------------
def _all_config_names():
    """All known config names. Add new entries here when creating a new API."""
    return [
        "all_squads",
        "entities",
        "fixtures",
        "ladder",
        "live_ladder",
        "livexy",
        "matchdata",
        "matchstats",
        "matchstatsandevents",
        "player_associations",
        "player_profile",
        "roundschedule",
        "season_list",
        "squad_list",
    ]

if not config_name or config_name.lower() == "all":
    config_names = _all_config_names()
    print(f"[runner_variant] Running ALL configs: {config_names}")
else:
    config_names = [config_name]

# COMMAND ----------

# ------------------------------------------------------------------
# Run ID — one UUID per notebook execution, shared across all configs
# in this run and written to both the audit log and error log.
#
# NOTE: create_error_log_table / create_audit_table have been removed.
#       Table creation is handled once by task_1_init_log_tables before
#       the ForEach fans out — eliminates MetadataChangedException from
#       concurrent DDL when 9+ iterations start simultaneously.
# ------------------------------------------------------------------
run_id = str(_uuid.uuid4())
print(f"[runner_variant] run_id = {run_id}")

# COMMAND ----------

# ------------------------------------------------------------------
# Load and run one config
# ------------------------------------------------------------------
def _run_config(name):
    module_path = f"configs.nrl_{name}_config"
    object_name = f"{name.upper()}_CONFIG"

    try:
        module = importlib.import_module(module_path)
    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            f"No config file found for '{name}' — expected module '{module_path}'. "
            f"Check the config name spelling or add the missing config file."
        )

    try:
        config = getattr(module, object_name)
    except AttributeError:
        raise ValueError(
            f"Config module '{module_path}' has no object named '{object_name}'. "
            f"Ensure the config variable is named exactly '{object_name}'."
        )

    print(f"\n{'='*60}")
    print(f"Running flatten job (VARIANT) — config: {name}")
    print(f"{'='*60}")

    config = dict(config)   # shallow copy — never mutate the module-level dict

    # --------------------------------------------------------------
    # Determine audit key for the log entry.
    # ForEach mode : use the composite key built by Task 2
    #                e.g. "all_squads_111_2025"
    # Direct mode  : plain config name — preserves original behaviour
    # --------------------------------------------------------------
    _audit_key = audit_key if is_foreach else name

    # --------------------------------------------------------------
    # Build bronze_filter
    #
    # Two independent parts are combined with AND:
    #
    #   Part 1 — source scope (competition / season)
    #     Rendered from source_filter_template when run_params are
    #     available.  Skipped for non-parametric configs (entities etc).
    #
    #   Part 2 — watermark window (incremental runs only)
    #     ingested_at > tgt_watermark AND ingested_at <= src_watermark
    #     Skipped on first run (tgt_watermark is None) so all rows are
    #     processed when building the target tables from scratch.
    #     Skipped entirely in direct mode (no watermark values present).
    # --------------------------------------------------------------
    bronze_filter_parts = []

    # Part 1: source scope
    template = config.get("source_filter_template")
    if template and run_params:
        required_keys = {
            fn
            for _, fn, _, _ in _string.Formatter().parse(template)
            if fn
        }
        if required_keys and required_keys.issubset(run_params):
            bronze_filter_parts.append(template.format(**run_params))
            print(f"[runner_variant] source filter : {bronze_filter_parts[-1]}")
        else:
            missing = required_keys - run_params.keys()
            print(
                f"[runner_variant] source_filter_template needs {missing} "
                f"but not all provided — no source filter for '{name}'"
            )

    # Part 2: watermark window
    if is_foreach and tgt_watermark is not None:
        bronze_filter_parts.append(
            f"ingested_at > '{tgt_watermark}' AND ingested_at <= '{src_watermark}'"
        )
        print(f"[runner_variant] watermark filter: ingested_at > '{tgt_watermark}' AND ingested_at <= '{src_watermark}'")
    elif is_foreach and tgt_watermark is None:
        print(f"[runner_variant] First run — no watermark filter, all bronze rows will be processed")

    if bronze_filter_parts:
        config["bronze_filter"] = " AND ".join(bronze_filter_parts)
        print(f"[runner_variant] bronze_filter : {config['bronze_filter']}")

    # --------------------------------------------------------------
    # Bronze is append-only — always add rows, never truncate existing data.
    # write_mode is set here and propagated to every table in the schema
    # tree via config["write_mode"].
    # --------------------------------------------------------------
    config["write_mode"] = "append"

    print(f"[runner_variant] write_mode    : {config['write_mode']}")

    # --------------------------------------------------------------
    # Run pipeline
    # All current configs use run_multi_table_pipeline (elements key).
    # --------------------------------------------------------------
    if "elements" not in config:
        print(
            f"[SKIP] Config '{name}' has no 'elements' key — "
            f"single-table configs are not supported in the VARIANT pipeline."
        )
        return

    # ------------------------------------------------------------------
    # Pre-flight: verify the bronze table actually exists before
    # handing off to the pipeline.  A misspelled bronze_table key
    # would otherwise silently produce 0-row silver tables with no
    # error, making it very hard to detect the misconfiguration.
    # ------------------------------------------------------------------
    _bronze_table = config.get("bronze_table", "")
    if _bronze_table and not spark.catalog.tableExists(_bronze_table):   # noqa: F821
        raise ValueError(
            f"[pre-flight] Bronze table '{_bronze_table}' does not exist. "
            f"Check the 'bronze_table' key in the '{name}' config."
        )

    run_multi_table_pipeline(config, spark)     # noqa: F821

    if "post_run_fn" in config:
        print(f"[runner_variant] Running post_run_fn for '{name}'...")
        config["post_run_fn"](config, spark)    # noqa: F821

    # Audit entry uses _audit_key as config_name so task_2_discover_configs
    # can look up the correct watermark on the next run.
    log_pipeline_audit(AUDIT_LOG_TABLE, run_id, _audit_key, config, spark)  # noqa: F821


# COMMAND ----------

errors = {}
for _name in config_names:
    try:
        _run_config(_name)
    except Exception as _e:
        errors[_name] = _e
        print(f"[ERROR] '{_name}' failed: {_e}")
        log_pipeline_error(ERROR_LOG_TABLE, run_id, _name, _e, spark)   # noqa: F821

if errors:
    failed = ", ".join(errors.keys())
    raise RuntimeError(
        f"The following configs failed: {failed}. "
        "See output above and the error log table for details."
    )
