# Databricks notebook source
"""
discover_configs.py
-------------------
First task in the Flattening POC ForEach workflow.

Outputs a JSON list of config names as a task value so that
the downstream ForEach task can fan out one runner.py execution per config.

Behaviour
---------
- config_name blank / "all"  → publishes the full CONFIG_NAMES list (all configs run in parallel)
- config_name = "all_squads" → publishes ["all_squads"] (ForEach runs a single iteration)

To add a new API endpoint: add its config_name to CONFIG_NAMES below.
No other job changes are needed.
"""

import json

# COMMAND ----------

# ------------------------------------------------------------------
# Master list of all config names.
# Each entry maps to:   configs/nrl_{name}_config.py
#                       {NAME}_CONFIG object inside that file
# ------------------------------------------------------------------
CONFIG_NAMES = [
    "all_squads",
    "entities",
    "ladder",
    "live_ladder",
    "live_xy",
    "player_associations",
    "player_profile",
    "season_list",
    "squad_list",
]

# COMMAND ----------

# ------------------------------------------------------------------
# Read the config_name job parameter.
# If a specific config is requested, publish only that one so the
# ForEach task runs a single iteration instead of the full list.
# ------------------------------------------------------------------
config_name = ""
try:
    dbutils.widgets.text("config_name", "")                       # noqa: F821
    config_name = dbutils.widgets.get("config_name").strip()      # noqa: F821
except Exception:
    pass

if config_name and config_name.lower() != "all":
    configs_to_run = [config_name]
    print(f"[discover_configs] config_name='{config_name}' — running single config")
else:
    configs_to_run = CONFIG_NAMES
    print(f"[discover_configs] config_name blank/all — running all {len(CONFIG_NAMES)} configs")

# COMMAND ----------

# ------------------------------------------------------------------
# Publish the list as a task value for the ForEach task to consume.
# Reference in the ForEach "Inputs" field:
#   {{tasks.discover_configs.values.configs}}
# ------------------------------------------------------------------
dbutils.jobs.taskValues.set(key="configs", value=json.dumps(configs_to_run))  # noqa: F821
print(f"[discover_configs] Published: {configs_to_run}")
