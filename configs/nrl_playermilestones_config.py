"""
NRL Player Milestones API — Flattening Config
Source  : api/NRL/playerMilestones/{competitionId}/{seasonId}
Bronze  : sandbox.swathi.playermilestones
Mode    : Standard (l0payload is NULL)
Element : l1element = 'stats'

ingested_from pattern : playerMilestones_{competitionId}_{seasonId}
e.g. playerMilestones_111_2026

Payload structure (array — IS the leaderboardStat array directly):
  [{
    "ID":          "76",             ← statId
    "statName":    "Points",
    "milestoneEntry": {              ← dict when single, array when multiple
      "PlayerID", "PlayerName",
      "TeamID",   "TeamName",
      "Value",    "RequiredAmount", "Achieved"
    }
  }, ...]

Output tables (target_prefix = sandbox.swathi.nrl_player_milestones):
  nrl_player_milestones              ← Level 0 root  (one row per ingestion)
  nrl_player_milestones_leaderboard_stat   ← Level 1  (one row per stat)
  nrl_player_milestones_milestone_entry    ← Level 2  (one row per player per stat)

NOTE: payload is a JSON array at root level, not a JSON object.
The framework writes a thin intermediate root table; all three doc-aligned
tables are created by post_run_fn.
"""

import json as _json
from pyspark.sql import functions as F


def _write_records(records, table_name, spark, write_mode="overwrite"):
    """Build an explicit all-STRING schema and write records to a Delta table."""
    if not records:
        print(f"[playermilestones] No records for {table_name} — skipping")
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
    print(f"[playermilestones] {len(rows)} rows → {table_name}")


def _create_player_milestones_tables(config, spark):
    """
    Creates three doc-aligned tables from the playerMilestones bronze data.

    nrl_player_milestones            — one row per competition+season
    nrl_player_milestones_leaderboard_stat  — one row per stat
    nrl_player_milestones_milestone_entry   — one row per player per stat
    """
    bronze  = config["bronze_table"]
    prefix  = config["target_prefix"]
    bf      = config.get("bronze_filter")

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
        )
        .collect()
    )

    root_records  = []
    stat_records  = []
    entry_records = []
    seen_root     = set()

    for row in rows:
        # Parse context from ingested_from: player_milestones_{competitionId}_{seasonId}
        parts          = row.ingested_from.split("_")
        competition_id = parts[2] if len(parts) > 2 else None
        season_id      = parts[3] if len(parts) > 3 else None

        ctx = {
            "ingested_from":  row.ingested_from,
            "ingested_at":    str(row.ingested_at),
            "competition_id": competition_id,
            "season_id":      season_id,
            "source_schema":       "bronze_stats",
            "source_table_name":   "player_milestones",
            "target_schema":       "bronze_stats_flatten",
        }

        root_key = (competition_id, season_id)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "target_table_name": "nrl_player_milestones",
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
                "target_table_name": "nrl_player_milestones_leaderboard_stat",
            })

            # milestoneEntry may be a single dict (one player) or a list
            milestone_entry = stat.get("milestoneEntry", [])
            if isinstance(milestone_entry, dict):
                milestone_entry = [milestone_entry]

            for entry in milestone_entry:
                if not isinstance(entry, dict):
                    continue
                entry_records.append({
                    "stat_id":         stat_id,
                    "stat_name":       stat_name,
                    "player_id":       entry.get("PlayerID"),
                    "player_name":     entry.get("PlayerName"),
                    "team_id":         entry.get("TeamID"),
                    "team_name":       entry.get("TeamName"),
                    "value":           entry.get("Value"),
                    "required_amount": entry.get("RequiredAmount"),
                    "achieved":        entry.get("Achieved"),
                    **ctx,
                    "target_table_name": "nrl_player_milestones_milestone_entry",
                })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,  f"{prefix}",                   spark, write_mode)
    _write_records(stat_records,  f"{prefix}_leaderboard_stat",  spark, write_mode)
    _write_records(entry_records, f"{prefix}_milestone_entry",   spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────

PLAYERMILESTONES_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.player_milestones",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_player_milestones",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    # source_filter_template is used by the runner to scope ingestion.
    # playerMilestones is season-level (no roundNumber in ingested_from).
    "source_filter_template": "ingested_from LIKE 'player_milestones_{competition_id}_{season}%'",

    # Payload is a direct array of leaderboardStat objects; exclude child
    # arrays from framework auto-discovery (all tables owned by post_run_fn).
    "exclude_fields": {
        "leaderboardStat",
        "milestoneEntry",
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

    # Creates: nrl_player_milestones,
    #          nrl_player_milestones_leaderboard_stat,
    #          nrl_player_milestones_milestone_entry
    "post_run_fn": _create_player_milestones_tables,
}
