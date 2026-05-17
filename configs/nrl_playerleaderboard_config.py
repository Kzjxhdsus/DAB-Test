"""
NRL Player Leaderboard API — Flattening Config
Source  : api/NRL/playerLeaderBoard/{competitionId}/{seasonId}/{roundNumber}
Bronze  : nrl_datalakehouse_qa.bronze_stats.player_leaderboards
Mode    : Standard (l0payload contains root attributes).
Element : l1element = 'stats'

ingested_from pattern : player_leaderboards_{competitionId}_{seasonId}_{roundNumber}
e.g. player_leaderboards_111_2026_1

l0payload (object — <playerLeaderboard> root attributes):
  { competitionId, competitionName, seasonId, roundNumber }

l1element = 'stats' payload — IS the leaderboardStat array directly:
  [{
    "ID":       "0",          ← statId
    "statName": "Sin Bin",
    "leaderboardEntry": [
      {
        TeamID, TeamName, TeamAbbrev,
        PlayerID, PlayerName,
        PositionID, PositionName,
        Value, Rank, Appearances
      }, ...
    ]
  }, ...]

Output tables (target_prefix = nrl_datalakehouse_qa.bronze_stats_flatten.nrl_player_lb):
  nrl_player_lb_leaderboard  ← Level 0  Root (one row per competition+season+round)
  nrl_player_lb_stat         ← Level 1  Stat category (one row per stat per round)
  nrl_player_lb_entry        ← Level 2  Player entry (one row per player per stat per round)

NOTE: all tables are created by post_run_fn.
      FK columns on child tables: competition_id, season_id, round_number (+ stat_id for entry).
      Names and display attrs (stat_name, player_name, team_abbrev etc.) live only at
      their own level — not propagated as FKs.
"""

import json as _json
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Writer helper
# ---------------------------------------------------------------------------
def _write_records(records, table_name, spark, write_mode="overwrite"):
    """Build an explicit all-STRING schema and write records to a Delta table."""
    if not records:
        print(f"[playerleaderboard] No records for {table_name} — skipping")
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
    print(f"[playerleaderboard] {len(rows)} rows → {table_name} (mode={write_mode})")


# ---------------------------------------------------------------------------
# post_run_fn
# ---------------------------------------------------------------------------
def _create_player_leaderboards_tables(config, spark):
    """
    Creates three doc-aligned tables from the playerLeaderboard bronze data.

    nrl_player_lb_leaderboard — one row per competition+season+round (root context)
    nrl_player_lb_stat        — one row per stat category per round
    nrl_player_lb_entry       — one row per player per stat per round
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
        # Parse context: player_leaderboards_{competitionId}_{seasonId}_{roundNumber}
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
            "ingested_from":     row.ingested_from,
            "ingested_at":       str(row.ingested_at),
            "competition_id":    competition_id,
            "season_id":         season_id,
            "round_number":      round_number,
            "source_schema":     "bronze_stats",
            "source_table_name": "player_leaderboards",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Level 0: Root row — one per competition+season+round ──────────────
        root_key = (competition_id, season_id, round_number)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name":  competition_name,
                "target_table_name": "nrl_player_lb_leaderboard",
            })

        # ── Parse payload — leaderboardStat array ─────────────────────────────
        payload = _json.loads(row.p) if row.p else {}
        if isinstance(payload, dict):
            inner   = payload.get("leaderboardStat")
            payload = (inner if isinstance(inner, list) else [inner]) if inner is not None else [payload]

        for stat in payload:
            if not isinstance(stat, dict):
                continue

            stat_id   = stat.get("ID")
            stat_name = stat.get("statName")

            # ── Level 1: Stat row ─────────────────────────────────────────────
            stat_records.append({
                "stat_id":           stat_id,
                "stat_name":         stat_name,
                **ctx,
                "target_table_name": "nrl_player_lb_stat",
            })

            # ── Level 2: Player entry rows ────────────────────────────────────
            leaderboard_entry = stat.get("leaderboardEntry", [])
            if isinstance(leaderboard_entry, dict):
                leaderboard_entry = [leaderboard_entry]

            for entry in leaderboard_entry:
                if not isinstance(entry, dict):
                    continue
                entry_records.append({
                    "stat_id":       stat_id,          # FK to stat level
                    "team_id":       entry.get("TeamID"),
                    "team_name":     entry.get("TeamName"),
                    "team_abbrev":   entry.get("TeamAbbrev"),
                    "player_id":     entry.get("PlayerID"),
                    "player_name":   entry.get("PlayerName"),
                    "position_id":   entry.get("PositionID"),
                    "position_name": entry.get("PositionName"),
                    "rank":          entry.get("Rank"),
                    "value":         entry.get("Value"),
                    "appearances":   entry.get("Appearances"),
                    **ctx,
                    "target_table_name": "nrl_player_lb_entry",
                })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,  f"{prefix}_leaderboard", spark, write_mode)
    _write_records(stat_records,  f"{prefix}_stat",        spark, write_mode)
    _write_records(entry_records, f"{prefix}_entry",       spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
PLAYERLEADERBOARD_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_qa.bronze_stats.player_leaderboards",
    "target_prefix": "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_player_lb",

    # Always append so historical loads accumulate correctly.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'player_leaderboards_{competition_id}_{season}%'",

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

    # Suppress framework-generated root table; all tables owned by post_run_fn.
    "exclude_root_aliases": {"stats"},

    # Creates: nrl_player_lb_leaderboard,
    #          nrl_player_lb_stat,
    #          nrl_player_lb_entry
    "post_run_fn": _create_player_leaderboards_tables,
}
