# Databricks notebook source
"""
task_1_init_log_tables.py
--------------------------
First task in the NRL Flattening Framework workflow.

Runs once per job execution, before the ForEach.

Responsibilities
----------------
1. Ensure the error log table exists (creates it on first run, no-op after).
2. Ensure the audit log table exists  (creates it on first run, no-op after).
3. Display both tables so the run log shows their current state.
4. Publish the fully-qualified table names as task values so that
   discover_configs (Task 2) and runner_variant (Task 3) never need to
   hardcode them — they read from taskValues instead.

Why a separate task?
--------------------
Previously runner_variant.py called create_error_log_table / create_audit_table
at the start of every ForEach iteration.  With 9 configs running in parallel
that caused MetadataChangedException (Unity Catalog concurrent DDL conflict)
on the very first run.  Moving DDL here — one task, one run — eliminates that.
"""

# COMMAND ----------

import importlib

import framework_variant as _fv
importlib.reload(_fv)
from framework_variant import create_error_log_table, create_audit_table

# COMMAND ----------

# ------------------------------------------------------------------
# Job parameters
# Passed as job-level parameters in the Databricks Workflow.
#
#   catalog       — Unity Catalog catalog name  (e.g. "sandbox")
#   target_schema — schema for all framework tables (e.g. "ashish")
# ------------------------------------------------------------------
catalog       = ""
target_schema = ""

try:
    catalog       = dbutils.widgets.get("catalog").strip()        # noqa: F821
    target_schema = dbutils.widgets.get("target_schema").strip()  # noqa: F821
except Exception:
    pass

if not catalog or not target_schema:
    raise ValueError(
        "Both 'catalog' and 'target_schema' job parameters must be set. "
        f"Got: catalog='{catalog}', target_schema='{target_schema}'"
    )

print(f"[task_1] catalog       = {catalog}")
print(f"[task_1] target_schema = {target_schema}")

# COMMAND ----------

# ------------------------------------------------------------------
# Derive fully-qualified table names
# ------------------------------------------------------------------
ERROR_LOG_TABLE = f"{catalog}.{target_schema}.nrl_pipeline_error_log"
AUDIT_LOG_TABLE = f"{catalog}.{target_schema}.nrl_pipeline_audit_log"

print(f"[task_1] error_log_table = {ERROR_LOG_TABLE}")
print(f"[task_1] audit_log_table = {AUDIT_LOG_TABLE}")

# COMMAND ----------

# ------------------------------------------------------------------
# Create tables (idempotent — CREATE TABLE IF NOT EXISTS)
# ------------------------------------------------------------------
create_error_log_table(ERROR_LOG_TABLE, spark)  # noqa: F821
create_audit_table(AUDIT_LOG_TABLE, spark)       # noqa: F821

# COMMAND ----------

# ------------------------------------------------------------------
# Display current state of both tables
# ------------------------------------------------------------------
print("\n--- Error Log ---")
spark.sql(f"SELECT * FROM {ERROR_LOG_TABLE} ORDER BY run_timestamp DESC LIMIT 20").show(  # noqa: F821
    truncate=False
)

print("\n--- Audit Log ---")
spark.sql(f"SELECT * FROM {AUDIT_LOG_TABLE} ORDER BY flattened_at DESC LIMIT 20").show(  # noqa: F821
    truncate=False
)

# COMMAND ----------

# ------------------------------------------------------------------
# Publish table names as task values for downstream tasks
# Downstream reads:
#   dbutils.jobs.taskValues.get(taskKey="task_1_init_log_tables",
#                               key="error_log_table")
#   dbutils.jobs.taskValues.get(taskKey="task_1_init_log_tables",
#                               key="audit_log_table")
# ------------------------------------------------------------------
dbutils.jobs.taskValues.set(key="error_log_table", value=ERROR_LOG_TABLE)  # noqa: F821
dbutils.jobs.taskValues.set(key="audit_log_table", value=AUDIT_LOG_TABLE)  # noqa: F821

print(f"\n[task_1] Published task values:")
print(f"         error_log_table → {ERROR_LOG_TABLE}")
print(f"         audit_log_table → {AUDIT_LOG_TABLE}")
