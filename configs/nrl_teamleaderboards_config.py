"""
NRL Team Leaderboards API — Flattening Config
Source  : api/NRL/teamLeaderBoards/{competitionId}/{seasonId}/{roundNumber}
Bronze  : sandbox.swathi.teamleaderboards
Mode    : Standard (l0payload is NULL)
Element : l1element = 'stats'

ingested_from pattern : teamLeaderboards_{competitionId}_{seasonId}_{roundNumber}
e.g. teamLeaderboards_111_2026_1

Payload structure (array — IS the leaderboardStat array directly):
  [{
    "ID":       "0",            ← statId
    "statName": "Sin Bin",
    "leaderboardEntry": [       ← array of team ranking rows
      {
        "TeamID", "TeamName", "TeamAbbrev",
        "Rank", "Value", "Appearances"
      }, ...
    ]
  }, ...]

Output tables (target_prefix = sandbox.swathi.nrl_team_leaderboards):
  nrl_team_leaderboards                      ← Level 0 root (one row per round ingestion)
  nrl_team_leaderboards_leaderboard_stat     ← Level 1  (one row per stat per round)
  nrl_team_leaderboards_leaderboard_entry    ← Level 2  (one row per team per stat per round)

NOTE: payload is a JSON array at root level — all three doc-aligned tables
are created by post_run_fn.
"""

import json as _json
from pyspark.sql import functions as F


def _write_records(records, table_name, spark, write_mode="overwrite"):
    """Build an explicit all-STRING schema and write records to a Delta table."""
    if not records:
        print(f"[teamleaderboards] No records for {table_name} — skipping")
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
    print(f"[teamleaderboards] {len(rows)} rows → {table_name}")


def _create_team_leaderboards_tables(config, spark):
    """
    Creates three doc-aligned tables from the teamLeaderboards bronze data.

    nrl_team_leaderboards                   — one row per competition+season+round
    nrl_team_leaderboards_leaderboard_stat  — one row per stat per round
    nrl_team_leaderboards_leaderboard_entry — one row per team per stat per round
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
        # Parse context: team_leaderboards_{competitionId}_{seasonId}_{roundNumber}
        parts          = row.ingested_from.split("_")
        competition_id = parts[2] if len(parts) > 2 else None
        season_id      = parts[3] if len(parts) > 3 else None
        round_number   = parts[4] if len(parts) > 4 else None

        competition_name = None
        if row.l0:
            try:
                l0 = _json.loads(row.l0)
                if isinstance(l0, dict):
                    competition_name = l0.get("competitionName")
            except Exception:
                pass

        ctx = {
            "ingested_from":  row.ingested_from,
            "ingested_at":    str(row.ingested_at),
            "competition_id": competition_id,
            "season_id":      season_id,
            "round_number":   round_number,
            "source_schema":       "bronze_stats",
            "source_table_name":   "team_leaderboards",
            "target_schema":       "bronze_stats_flatten",
        }

        root_key = (competition_id, season_id, round_number, competition_name)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name": competition_name,
                "target_table_name": "nrl_team_leaderboards",
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
                "target_table_name": "nrl_team_leaderboards_leaderboard_stat",
            })

            leaderboard_entry = stat.get("leaderboardEntry", [])
            if isinstance(leaderboard_entry, dict):
                leaderboard_entry = [leaderboard_entry]

            for entry in leaderboard_entry:
                if not isinstance(entry, dict):
                    continue
                entry_records.append({
                    "stat_id":      stat_id,
                    "stat_name":    stat_name,
                    "team_id":      entry.get("TeamID"),
                    "team_name":    entry.get("TeamName"),
                    "team_abbrev":  entry.get("TeamAbbrev"),
                    "rank":         entry.get("Rank"),
                    "value":        entry.get("Value"),
                    "appearances":  entry.get("Appearances"),
                    **ctx,
                    "target_table_name": "nrl_team_leaderboards_leaderboard_entry",
                })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,  f"{prefix}",                    spark, write_mode)
    _write_records(stat_records,  f"{prefix}_leaderboard_stat",   spark, write_mode)
    _write_records(entry_records, f"{prefix}_leaderboard_entry",  spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────

TEAMLEADERBOARDS_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.team_leaderboards",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_team_leaderboards",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'team_leaderboards_{competition_id}_{season}%'",

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

    # Creates: nrl_team_leaderboards,
    #          nrl_team_leaderboards_leaderboard_stat,
    #          nrl_team_leaderboards_leaderboard_entry
    "post_run_fn": _create_team_leaderboards_tables,
}
