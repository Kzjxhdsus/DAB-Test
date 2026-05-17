"""
NRL Competition Team Career Best Leaderboard Report API — Flattening Config
Source  : api/NRL/competitionTeamCareerBestLeaderboardReport/{competitionId}
Bronze  : sandbox.swathi.competitionteamcareerbestleaderboardreport
Mode    : Standard (l0payload is NULL)
Element : l1element = 'stats'

ingested_from pattern : competitionTeamCareerBestLeaderboardReport_{competitionId}
e.g. competitionTeamCareerBestLeaderboardReport_111

XML root: <teamLeaderboard seasonId="0" competitionId="111" roundNumber="0">
NOTE: seasonId is always "0" for career-best (all-time) data.
NOTE: No season in ingested_from — competition-level, all-time data.

Payload structure (array of leaderboardStat objects):
  [{
    "ID":       "0",                 ← statId
    "statName": "Sin Bin",
    "leaderboardEntry": [            ← career-best entries (one per team/season/round combination)
      {
        "TeamID",    "TeamName",    "TeamAbbrev",
        "Value",     "Rank",        "Appearances",
        "SeasonId",                  ← the season when this career-best occurred
        "RoundId",                   ← the round when it occurred
        "OpponentId","OpponentName"  ← the opponent in that match
      }, ...
    ]
  }, ...]

Key difference from competitionTeamHistoryLeaderboardReport: each entry carries the
season/round/opponent context for the specific match where the career best was set.

Output tables (target_prefix = sandbox.swathi.nrl_career_best_lb):
  nrl_career_best_lb_leaderboard  ← Level 0 root (one row per competition)
  nrl_career_best_lb_stat         ← Level 1 (one row per stat per competition)
  nrl_career_best_lb_entry        ← Level 2 (one row per team/season/round career-best entry)

NOTE: payload is a JSON array at root level — all three tables are created
by post_run_fn.
"""

import json as _json
from pyspark.sql import functions as F


def _write_records(records, table_name, spark, write_mode="overwrite"):
    """Build an explicit all-STRING schema and write records to a Delta table."""
    if not records:
        print(f"[competitionteamcareerbestleaderboardreport] No records for {table_name} — skipping")
        return
    from pyspark.sql.types import StructType, StructField, StringType
    keys   = list(dict.fromkeys(k for r in records for k in r))
    schema = StructType([StructField(k, StringType(), True) for k in keys])
    rows   = [
        {k: (str(r[k]) if k in r and r[k] is not None else None) for k in keys}
        for r in records
    ]
    (
        spark.createDataFrame(rows, schema=schema)
        .write.format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"[competitionteamcareerbestleaderboardreport] {len(rows)} rows → {table_name}")


def _create_career_best_lb_tables(config, spark):
    """
    Creates three doc-aligned tables from the competitionTeamCareerBestLeaderboardReport bronze data.

    nrl_career_best_lb_leaderboard — one row per competition (seasonId always "0" = all-time)
    nrl_career_best_lb_stat        — one row per stat per competition
    nrl_career_best_lb_entry       — one row per team/season/round career-best entry
    """
    bronze = config["bronze_table"]
    prefix = config["target_prefix"]
    bf     = config.get("bronze_filter")

    bronze_df = spark.table(bronze)
    if bf:
        bronze_df = bronze_df.filter(bf)

    rows = (
        bronze_df
        .filter(F.col("l1element") == "stats")
        .select(
            "ingested_from",
            "ingested_at",
            F.col("payload").cast("string").alias("p"),
            F.col("l0payload").cast("string").alias("l0"),
        )
        .collect()
    )

    root_records  = []
    stat_records  = []
    entry_records = []
    seen_root     = set()

    for row in rows:
        # Parse context: career_best_leaderboard_{competitionId}
        parts          = row.ingested_from.split("_")
        competition_id = parts[3] if len(parts) > 3 else None
        round_number   = parts[4] if len(parts) > 4 else None

        # Extract competition_name from l0payload if available
        competition_name = None
        if row.l0:
            try:
                l0 = _json.loads(row.l0)
                if isinstance(l0, dict):
                    competition_name = l0.get("competitionName")
            except Exception:
                pass

        ctx = {
            "ingested_from":       row.ingested_from,
            "ingested_at":         str(row.ingested_at),
            "competition_id":      competition_id,
            "season_id":           "0",    # career-best = all-time; root XML seasonId="0"
            "round_number":        "0",    # career-best = all-time; root XML round_number="0"
            "source_schema":       "bronze_stats",
            "source_table_name":   "career_best_leaderboard",
            "target_schema":       "bronze_stats_flatten",
        }

        root_key = (competition_id, round_number, competition_name)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name":  competition_name,
                "target_table_name": "nrl_career_best_lb_leaderboard",
            })

        payload = _json.loads(row.p) if row.p else {}
        if isinstance(payload, dict):
            inner = payload.get("leaderboardStat")
            payload = (inner if isinstance(inner, list) else [inner]) if inner is not None else [payload]

        for stat in payload:
            if not isinstance(stat, dict):
                continue

            stat_id   = stat.get("ID")
            stat_name = stat.get("statName")

            stat_records.append({
                "stat_id":   stat_id,
                "stat_name": stat_name,
                **ctx,
                "target_table_name": "nrl_career_best_lb_stat",
            })

            leaderboard_entry = stat.get("leaderboardEntry", [])
            if isinstance(leaderboard_entry, dict):
                leaderboard_entry = [leaderboard_entry]

            for entry in leaderboard_entry:
                if not isinstance(entry, dict):
                    continue
                entry_records.append({
                    "stat_id":        stat_id,
                    "stat_name":      stat_name,
                    "team_id":        entry.get("TeamID"),
                    "team_name":      entry.get("TeamName"),
                    "team_abbrev":    entry.get("TeamAbbrev"),
                    "value":          entry.get("Value"),
                    "rank":           entry.get("Rank"),
                    "appearances":    entry.get("Appearances"),
                    "match_season_id": entry.get("SeasonId"),   # season the career best was set
                    "round_id":       entry.get("RoundId"),     # round the career best was set
                    "opponent_id":    entry.get("OpponentId"),
                    "opponent_name":  entry.get("OpponentName"),
                    **ctx,
                    "target_table_name": "nrl_career_best_lb_entry",
                })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,  f"{prefix}_leaderboard",  spark, write_mode)
    _write_records(stat_records,  f"{prefix}_stat",        spark, write_mode)
    _write_records(entry_records, f"{prefix}_entry",       spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────

COMPETITIONTEAMCAREERBESTLEADERBOARDREPORT_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.career_best_leaderboard",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_career_best_lb",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    # Filter by competition_id only — no season in ingested_from for this API
    "source_filter_template": "ingested_from LIKE 'career_best_leaderboard_{competition_id}%'",

    "exclude_fields": {
        "leaderboardStat",
        "leaderboardEntry",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "stats": "stats",
    },

    "dedup_keys_map": {
        "stats": ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Suppress framework-generated root table for 'stats'; all tables owned by post_run_fn.
    "exclude_root_aliases": {"stats"},

    # Creates: nrl_career_best_lb_leaderboard,
    #          nrl_career_best_lb_stat,
    #          nrl_career_best_lb_entry
    "post_run_fn": _create_career_best_lb_tables,
}
