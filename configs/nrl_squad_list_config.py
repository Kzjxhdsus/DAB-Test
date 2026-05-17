"""
NRL Squad List API — Flattening Config
Source : api/NRL/competitions/squads/{competitionId}/{seasonId}
Bronze : one l1element (Squads), teams with player & coach sub-arrays.
         teamHistory is a flat struct on the team — inlined as columns
         via flatten_structs rather than exploded into a separate table.

Output tables  (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.squad):
  nrl_squad_squads   <- root  NOTE: design doc names this nrl_squad;
                        rename after load if exact naming is required.
  nrl_squad_team     <- Level 1  (one row per team)
  nrl_squad_player   <- Level 2 sibling
  nrl_squad_coach    <- Level 2 sibling
  nrl_squad_competition <- competition-level kicking success rates (post_run_fn)

NOTE: verify bronze_table name with the data engineering team.
"""

import json as _json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType as _ST, StructField as _SF, StringType as _S


def _create_squad_competition_table(config, spark):
    """
    Extracts competition-level scalar fields from l0payload that are not
    meaningful as inherited ancestor columns on team/player/coach rows
    (competitionEasyKickingSuccessRate, competitionToughKickingSuccessRate).
    Writes one row per competition/season ingestion to squad_competition.
    """
    bronze        = config["bronze_table"]
    target_prefix = config["target_prefix"]
    write_mode    = config.get("write_mode", "overwrite")

    _bronze_df = spark.table(bronze)
    _bf = config.get("bronze_filter")
    if _bf:
        _bronze_df = _bronze_df.filter(_bf)

    rows = (
        _bronze_df
        .select(
            F.col("l0payload").cast("string").alias("l0"),
            "ingested_from",
            "ingested_at",
        )
        .collect()
    )

    print(f"[squad post_run_fn] {len(rows)} rows to process")

    competition_records = []

    for row in rows:
        l0 = _json.loads(row.l0) if row.l0 else {}
        competition_records.append({
            "competition_id":                        l0.get("competitionId"),
            "competition_name":                      l0.get("competitionName"),
            "season_id":                             l0.get("seasonId"),
            "competition_easy_kicking_success_rate": l0.get("competitionEasyKickingSuccessRate"),
            "competition_tough_kicking_success_rate":l0.get("competitionToughKickingSuccessRate"),
            "ingested_from":                         row.ingested_from,
            "ingested_at":                           str(row.ingested_at),
        })

    if not competition_records:
        print(f"[squad post_run_fn] No records — skipping {target_prefix}_competition")
        return

    _keys   = list(dict.fromkeys(k for r in competition_records for k in r))
    _schema = _ST([_SF(k, _S(), True) for k in _keys])
    _rows   = [{k: (str(r[k]) if k in r and r[k] is not None else None) for k in _keys} for r in competition_records]
    (
        spark.createDataFrame(_rows, schema=_schema)
        .write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .saveAsTable(f"{target_prefix}_competition")
    )
    print(f"[squad post_run_fn] {len(_rows)} rows -> {target_prefix}_competition (mode={write_mode})")


SQUAD_LIST_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.squad",
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.squad",

    "exclude_fields": {
        "competitionEasyKickingSuccessRate",
        "competitionToughKickingSuccessRate",
    },

    "flatten_structs": {"teamHistory": "team_history_"},

    "force_array_fields": {"coach", "team"},

    "elements": {
        "teams": "teams",
    },

    "dedup_keys_map": {
        "squads":  ["competition_id", "season_id"],
        "team":    ["team_id"],
        "player":  ["player_id"],
        "coach":   ["coach_id"],
    },

    "source_filter_template": "ingested_from LIKE 'squad_{competition_id}_{season}%'",

    "sample_size":    200,
    "type_overrides": {},

    "post_run_fn": _create_squad_competition_table,
}