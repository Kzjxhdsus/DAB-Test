"""
NRL Competition Team Top Three Leaderboard Report API — Flattening Config
Source  : api/NRL/competitionTeamTopThreeLeaderboardReport/{competitionId}
Bronze  : sandbox.swathi.competitionteamtopthreeleaderboardreport
Mode    : Standard (l0payload is NULL)
Element : l1element = 'teams'

ingested_from pattern : competitionTeamTopThreeLeaderboardReport_{competitionId}
e.g. competitionTeamTopThreeLeaderboardReport_111

XML root: <teamTopThreeLeaderboard competitionId="111" competitionName="...">

Payload structure (array of team objects — one per NRL team including historics):
  [{
    "TeamAbbreviation": "AD",
    "TeamID":           "1",
    "TeamName":         "Adelaide",
    "stats": {
      "stat": [                         ← array of stat categories
        {
          "ID":       "0",              ← statId
          "statName": "Sin Bin",
          "statEntry": [                ← top-3 entries for this stat (usually up to 3 per season)
            {"GameID": "...", "Rank": "1", "SeasonId": "1998", "Value": "1"},
            {"GameID": "...", "Rank": "1", "SeasonId": "1998", "Value": "1"},
            {"GameID": "...", "Rank": "3", "SeasonId": "1998", "Value": "1"}
          ]
        }, ...
      ]
    }
  }, ...]

Nesting: competition → team → stats.stat[] → statEntry[]

Output tables (target_prefix = sandbox.swathi.nrl_top_three_lb):
  nrl_top_three_lb_leaderboard  ← Level 0 root (one row per competition)
  nrl_top_three_lb_team         ← Level 1 (one row per team per competition)
  nrl_top_three_lb_stat         ← Level 2 (one row per stat per team per competition)
  nrl_top_three_lb_entry        ← Level 3 (one row per top-3 stat entry — GameID/Rank/SeasonId/Value)

NOTE: payload is a JSON array at root level — all four tables are created
by post_run_fn. The stats are nested as stats.stat[] within each team object;
statEntry items are normalised (single dict → list of one).
"""

import json as _json
from pyspark.sql import functions as F


def _to_list(val):
    """Normalise a value that may be a list, a dict (single element), or absent."""
    if isinstance(val, list):
        return val
    if isinstance(val, dict):
        return [val]
    return []


def _write_records(records, table_name, spark, write_mode="overwrite"):
    """Build an explicit all-STRING schema and write records to a Delta table."""
    if not records:
        print(f"[competitionteamtopthreeleaderboardreport] No records for {table_name} — skipping")
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
    print(f"[competitionteamtopthreeleaderboardreport] {len(rows)} rows → {table_name}")


def _create_top_three_lb_tables(config, spark):
    """
    Creates four doc-aligned tables from the competitionTeamTopThreeLeaderboardReport bronze data.

    nrl_top_three_lb_leaderboard — one row per competition
    nrl_top_three_lb_team        — one row per team per competition
    nrl_top_three_lb_stat        — one row per stat per team per competition
    nrl_top_three_lb_entry       — one row per top-3 stat entry (GameID, Rank, SeasonId, Value)
    """
    bronze = config["bronze_table"]
    prefix = config["target_prefix"]
    bf     = config.get("bronze_filter")

    bronze_df = spark.table(bronze)
    if bf:
        bronze_df = bronze_df.filter(bf)

    rows = (
        bronze_df
        .filter(F.col("l1element") == "teams")
        .select(
            "ingested_from",
            "ingested_at",
            F.col("payload").cast("string").alias("p"),
            F.col("l0payload").cast("string").alias("l0"),
        )
        .collect()
    )

    root_records  = []
    team_records  = []
    stat_records  = []
    entry_records = []
    seen_root     = set()

    for row in rows:
        # Parse context: top_three_leaderboard_{competitionId}
        parts          = row.ingested_from.split("_")
        competition_id = parts[3] if len(parts) > 3 else None

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
            "source_schema":       "bronze_stats",
            "source_table_name":   "top_three_leaderboard",
            "target_schema":       "bronze_stats_flatten",
        }

        root_key = (competition_id, competition_name)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name":    competition_name,
                "target_table_name": "nrl_top_three_lb_leaderboard",
            })

        payload = _json.loads(row.p) if row.p else []
        # Payload is {"team": [...]} when XML-converted; normalise to list of team dicts
        if isinstance(payload, dict):
            payload = _to_list(payload.get("team") or payload)
        else:
            payload = _to_list(payload)

        # ---- Level 1: teams ----
        for team in payload:
            if not isinstance(team, dict):
                continue

            team_id   = team.get("TeamID")
            team_name = team.get("TeamName")
            team_abbr = team.get("TeamAbbreviation")

            team_records.append({
                "team_id":    team_id,
                "team_name":  team_name,
                "team_abbrev": team_abbr,
                **ctx,
                "target_table_name": "nrl_top_three_lb_team",
            })

            team_ctx = {**ctx, "team_id": team_id, "team_name": team_name}

            # ---- Level 2: stats.stat[] ----
            stats_obj = team.get("stats", {}) or {}
            stats     = stats_obj.get("stat", [])
            if isinstance(stats, dict):
                stats = [stats]

            for stat in stats:
                if not isinstance(stat, dict):
                    continue

                stat_id   = stat.get("ID")
                stat_name = stat.get("statName")

                stat_records.append({
                    "stat_id":   stat_id,
                    "stat_name": stat_name,
                    **team_ctx,
                    "target_table_name": "nrl_top_three_lb_stat",
                })

                stat_ctx = {**team_ctx, "stat_id": stat_id, "stat_name": stat_name}

                # ---- Level 3: statEntry[] ----
                stat_entries = stat.get("statEntry", [])
                if isinstance(stat_entries, dict):
                    stat_entries = [stat_entries]

                for entry in stat_entries:
                    if not isinstance(entry, dict):
                        continue
                    entry_records.append({
                        "game_id":   entry.get("GameID"),
                        "rank":      entry.get("Rank"),
                        "season_id": entry.get("SeasonId"),
                        "value":     entry.get("Value"),
                        **stat_ctx,
                        "target_table_name": "nrl_top_three_lb_entry",
                    })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,  f"{prefix}_leaderboard",  spark, write_mode)
    _write_records(team_records,  f"{prefix}_team",        spark, write_mode)
    _write_records(stat_records,  f"{prefix}_stat",        spark, write_mode)
    _write_records(entry_records, f"{prefix}_entry",       spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────

COMPETITIONTEAMTOPTHREELEADERBOARDREPORT_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.top_three_leaderboard",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_top_three_lb",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    # Filter by competition_id only — no season in ingested_from for this API
    "source_filter_template": "ingested_from LIKE 'top_three_leaderboard_{competition_id}%'",

    # Exclude nested array fields from framework auto-discovery.
    # All output tables are produced by post_run_fn which parses the full
    # 3-level nesting (team → stats.stat[] → statEntry[]) directly.
    "exclude_fields": {
        "team",
        "stats",
        "stat",
        "statEntry",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "teams": "teams",
    },

    "dedup_keys_map": {
        "teams": ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Suppress framework-generated root table for 'teams'; all tables owned by post_run_fn.
    "exclude_root_aliases": {"teams"},

    # Creates: nrl_top_three_lb_leaderboard,
    #          nrl_top_three_lb_team,
    #          nrl_top_three_lb_stat,
    #          nrl_top_three_lb_entry
    "post_run_fn": _create_top_three_lb_tables,
}
