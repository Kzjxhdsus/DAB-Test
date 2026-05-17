# Databricks notebook source
"""
task_2_discover_configs.py
--------------------------
Second task in the NRL Flattening Framework workflow.

Replaces the old discover_configs.py.

Responsibilities
----------------
1. Read job parameters: which configs, which competitions, which seasons.
2. For each (config, competition_id, season) combination:
     a. Query MAX(ingested_at) from the bronze table         → src_watermark
     b. Query MAX(ingested_at) from the audit log            → tgt_watermark
     c. If src > tgt (or tgt is None)  → new data exists, include in run list
     d. If src == tgt                  → nothing new, skip with a log message
3. Publish the run list as a task value so the ForEach task fans out one
   runner.py execution per entry.

ForEach input format (one JSON object per iteration):
    {
        "config_name":    "all_squads",
        "competition_id": "111",
        "season":         "2025",
        "audit_key":      "all_squads_111_2025",
        "src_watermark":  "2026-04-10T08:00:00",
        "tgt_watermark":  "2026-04-09T06:00:00"   (null on first run)
    }

Audit key convention (Option A — composite string):
    Parametric config  (comp + season) : "all_squads_111_2025"
    Parametric config  (comp only)     : "season_list_111"
    Non-parametric config              : "entities"

This key is stored as config_name in the audit log so each
(config, competition, season) combination has its own watermark history.
"""

import json
import importlib
import string as _string
import uuid as _uuid

import framework_variant as _fv
importlib.reload(_fv)
from framework_variant import log_pipeline_error

# COMMAND ----------

# ------------------------------------------------------------------
# Master list of all known configs.
# Keep in sync with runner.py CONFIG_NAMES.
# ------------------------------------------------------------------
CONFIG_NAMES = [
    "all_squads",
    "entities",
    "fixtures",
    "ladder",
    "live_ladder",
    "livexy",
    "matchdata",
    "matchstatsandevents",
    "player_associations",
    "player_profile",
    "roundschedule",
    "season_list",
    "squad_list",
]

# COMMAND ----------

# ------------------------------------------------------------------
# Read log table names published by Task 1.
# Fallback values used when running the notebook directly (no workflow).
# ------------------------------------------------------------------
try:
    AUDIT_LOG_TABLE = dbutils.jobs.taskValues.get(      # noqa: F821
        taskKey = "Initialize_Log_Tables",
        key     = "audit_log_table",
    )
    ERROR_LOG_TABLE = dbutils.jobs.taskValues.get(      # noqa: F821
        taskKey = "Initialize_Log_Tables",
        key     = "error_log_table",
    )
    print("[task_2] Log table names read from Task 1 task values")
except Exception:
    AUDIT_LOG_TABLE = "sandbox.ashish.nrl_pipeline_audit_log"
    ERROR_LOG_TABLE = "sandbox.ashish.nrl_pipeline_error_log"
    print("[task_2] Log table names from fallback (direct / interactive run)")

print(f"[task_2] audit_log_table = {AUDIT_LOG_TABLE}")
print(f"[task_2] error_log_table = {ERROR_LOG_TABLE}")

# One run_id for the whole discovery task — written to the error log
# for any watermark query failures so they appear in the same table
# as runner_variant errors.
run_id = str(_uuid.uuid4())
print(f"[task_2] run_id = {run_id}")

# COMMAND ----------

# ------------------------------------------------------------------
# Read job parameters
# ------------------------------------------------------------------
config_name    = ""
competition_id = ""
season         = ""

try:
    config_name    = dbutils.widgets.get("config_name").strip()     # noqa: F821
    competition_id = dbutils.widgets.get("competition_id").strip()  # noqa: F821
    season         = dbutils.widgets.get("season").strip()          # noqa: F821
except Exception:
    pass

print(f"[task_2] config_name    = '{config_name}'")
print(f"[task_2] competition_id = '{competition_id}'")
print(f"[task_2] season         = '{season}'")

# COMMAND ----------

# ------------------------------------------------------------------
# Parse comma-separated competition_id and season into lists.
# e.g.  "111,105"      → ["111", "105"]
#       "2024,2025"    → ["2024", "2025"]
#       ""             → []
# ------------------------------------------------------------------
competition_ids = [c.strip() for c in competition_id.split(",") if c.strip()]
seasons         = [s.strip() for s in season.split(",")         if s.strip()]

print(f"[task_2] competition_ids = {competition_ids}")
print(f"[task_2] seasons         = {seasons}")

# Which configs to evaluate — single, or full list
if config_name and config_name.lower() != "all":
    configs_to_check = [config_name]
    print(f"[task_2] Checking single config: {config_name}")
else:
    configs_to_check = CONFIG_NAMES
    print(f"[task_2] Checking all {len(CONFIG_NAMES)} configs")

# COMMAND ----------

# ------------------------------------------------------------------
# Helper: MAX(ingested_at) from bronze table
# bronze_filter is an optional SQL WHERE clause, e.g.:
#   "ingested_from = 'allSquads_111_2025'"
# ------------------------------------------------------------------
def get_src_watermark(bronze_table, bronze_filter):
    """
    Returns MAX(ingested_at) from bronze_table, optionally filtered.
    Returns None (not an error) when the table exists but contains no rows
    matching the filter — e.g. the first time a competition/season is ingested.
    Raises on any SQL or catalog error so the caller can treat it as a failure
    rather than silently skipping the config.
    """
    where = f"WHERE {bronze_filter}" if bronze_filter else ""
    try:
        row = spark.sql(                                            # noqa: F821
            f"SELECT MAX(ingested_at) AS max_ts FROM {bronze_table} {where}"
        ).collect()[0]
        return row["max_ts"]   # None only when table is empty / no matching rows
    except Exception as e:
        print(f"[get_src_watermark] ERROR reading {bronze_table}: {e}")
        raise   # re-raise — caller must not treat a SQL error as a legitimate skip


# ------------------------------------------------------------------
# Helper: MAX(ingested_at) from audit log for a given audit_key.
# Returns None if no previous run exists (first-run case).
# ------------------------------------------------------------------
def get_tgt_watermark(audit_key):
    row = (                                                         # noqa: F821
        spark.sql(f"""
            SELECT COUNT(*)         AS cnt,
                   MAX(ingested_at) AS max_ts
            FROM   {AUDIT_LOG_TABLE}
            WHERE  config_name = '{audit_key}'
        """)
        .collect()[0]
    )
    result = row["max_ts"]
    print(
        f"[get_tgt_watermark] '{audit_key}' → "
        f"rows={row['cnt']}  ingested_at={result}"
    )
    return result

# COMMAND ----------

# ------------------------------------------------------------------
# Main loop: evaluate each (config, competition_id, season) combination
# ------------------------------------------------------------------
run_list   = []
wm_errors  = {}   # audit_key → exception; collected so all failures are visible

for name in configs_to_check:

    # --- Load config to read bronze_table + source_filter_template ----
    module_path = f"configs.nrl_{name}_config"
    try:
        module = importlib.import_module(module_path)
        importlib.reload(module)
    except ModuleNotFoundError:
        print(f"[SKIP] {name}: config file not found at '{module_path}'")
        continue

    config       = getattr(module, f"{name.upper()}_CONFIG")
    bronze_table = config.get("bronze_table")
    template     = config.get("source_filter_template")   # None for non-parametric

    if not bronze_table:
        print(f"[SKIP] {name}: no 'bronze_table' defined in config")
        continue

    # --- Determine which parameter keys the template needs -------------
    # e.g. "ingested_from = 'allSquads_{competition_id}_{season}'"
    #       → required_keys = {"competition_id", "season"}
    # e.g. "ingested_from = 'seasonList_{competition_id}'"
    #       → required_keys = {"competition_id"}
    # e.g. None (entities, player_associations)
    #       → required_keys = set()
    required_keys = set()
    if template:
        required_keys = {
            field_name
            for _, field_name, _, _ in _string.Formatter().parse(template)
            if field_name
        }

    # --- Build list of (comp, season) combinations this config needs ---
    # Non-parametric (entities)            → [("", "")]
    # Needs comp only (season_list)        → [("111",""), ("105","")]
    # Needs comp + season (all_squads)     → [("111","2024"),("111","2025"),...]
    if not required_keys:
        param_combos = [("", "")]

    elif required_keys == {"competition_id"}:
        if not competition_ids:
            print(
                f"[SKIP] {name}: requires competition_id but none provided "
                f"(set the competition_id job parameter)"
            )
            continue
        param_combos = [(c, "") for c in competition_ids]

    else:   # requires both competition_id and season
        if not competition_ids or not seasons:
            print(
                f"[SKIP] {name}: requires competition_id + season but "
                f"competition_ids={competition_ids}, seasons={seasons} — "
                f"set both job parameters"
            )
            continue
        param_combos = [
            (comp, szn)
            for comp in competition_ids
            for szn  in seasons
        ]

    # --- Watermark check for each combination -------------------------
    for (comp, szn) in param_combos:

        # Build bronze filter for this combination
        if template and required_keys:
            params         = {}
            if comp: params["competition_id"] = comp
            if szn:  params["season"]         = szn
            bronze_filter  = template.format(**params)
        else:
            bronze_filter  = None

        # Build audit key (Option A composite string)
        key_parts = [name]
        if comp: key_parts.append(comp)
        if szn:  key_parts.append(szn)
        audit_key = "_".join(key_parts)

        # Watermark comparison
        try:
            src_wm = get_src_watermark(bronze_table, bronze_filter)
        except Exception as _e:
            wm_errors[audit_key] = _e
            print(f"  → ERROR querying bronze table — logged, continuing to next config")
            continue

        tgt_wm = get_tgt_watermark(audit_key)

        print(
            f"[{audit_key}]  "
            f"src={src_wm}  tgt={tgt_wm}  "
            f"bronze_filter={bronze_filter!r}"
        )

        if src_wm is None:
            print(f"  → SKIP: bronze table has no rows matching the filter (empty / not yet ingested)")
            continue

        if tgt_wm is not None and src_wm <= tgt_wm:
            print(f"  → SKIP: no new data since last run")
            continue

        run_list.append({
            "config_name":    name,
            "competition_id": comp,
            "season":         szn,
            "audit_key":      audit_key,
            "src_watermark":  str(src_wm),
            "tgt_watermark":  str(tgt_wm) if tgt_wm is not None else None,
        })
        print(f"  → QUEUED for flattening")

# COMMAND ----------

# ------------------------------------------------------------------
# Fail now if any bronze watermark queries errored.
# Raising before publishing the task value keeps the ForEach from
# starting at all, making it obvious that discovery itself failed.
# ------------------------------------------------------------------
if wm_errors:
    for _key, _exc in wm_errors.items():
        log_pipeline_error(ERROR_LOG_TABLE, run_id, _key, _exc, spark)  # noqa: F821
    msg_lines = [f"  {k}: {v}" for k, v in wm_errors.items()]
    raise RuntimeError(
        f"[task_2] {len(wm_errors)} watermark query error(s) — "
        f"fix the source_filter_template or bronze_table key in the affected config(s):\n"
        + "\n".join(msg_lines)
    )

# ------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------
print(f"\n[task_2] {len(run_list)} combination(s) queued for flattening:")
for entry in run_list:
    print(
        f"  {entry['audit_key']:<30}  "
        f"src={entry['src_watermark']}  "
        f"tgt={entry['tgt_watermark']}"
    )

skipped = sum(
    len([
        (c, s)
        for c in (competition_ids or [""])
        for s in (seasons or [""])
    ])
    for _ in configs_to_check
) - len(run_list)

if not run_list:
    print("\n[task_2] Nothing to do — all bronze tables are up to date.")

# COMMAND ----------

# ------------------------------------------------------------------
# Publish run list as task value for the ForEach task.
# Reference in ForEach "Inputs" field:
#   {{tasks.task_2_discover_configs.values.configs}}
# ------------------------------------------------------------------
dbutils.jobs.taskValues.set(                                        # noqa: F821
    key   = "configs",
    value = json.dumps(run_list),
)
print(f"\n[task_2] Published task value 'configs' — {len(run_list)} entries")
