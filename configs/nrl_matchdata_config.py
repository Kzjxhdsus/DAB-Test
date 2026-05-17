"""
NRL Match Data API — Flattening Config
Source : nrl_datalakehouse_prd.bronze_stats
Bronze : two l1elements —

  "gameInfo" (L0 mode)
    l0payload : game-level scalar attrs (gameId, competitionId, seasonId,
                city, refereeIds, weather, broadcastInfo, etc.)
    payload   : broadcast regions array

  "teams" (L0 mode)
    l0payload : game-level scalar attrs (gameId, competitionId, seasonId, etc.)
    payload   : array of team objects, each containing:
                score (struct -> inlined via flatten_structs),
                teamCoaches.teamCoach (single coach -> force_array_fields),
                teamLineup.insAndOuts.insOuts (ins/outs array),
                teamLineup.injuriesAndSuspensions (empty struct -> excluded),
                teamLineup.teamPlayer[] (per-player stats -> post_run_fn)

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.match_stats):
  match_stats_gameinfo             <- one row per game
  match_stats_teams                <- one row per team per game (includes score_* columns)
  match_stats_teams_teamcoach      <- coach per team per game
  match_stats_teams_insouts        <- player ins/outs per team
  match_stats_broadcaster          <- individual broadcasters per region per game (post_run_fn)
  match_stats_teams_team_player    <- per-player stats per team per game (post_run_fn)
"""

import json as _json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType as _ST, StructField as _SF, StringType as _S


def _create_matchdata_tables(config, spark):
    bronze        = config["bronze_table"]
    target_prefix = config["target_prefix"]
    write_mode    = config.get("write_mode", "overwrite")

    _bronze_df = spark.table(bronze)
    _bf = config.get("bronze_filter")
    if _bf:
        _bronze_df = _bronze_df.filter(_bf)

    # ── gameInfo rows ────────────────────────────────────────────────────────
    gameinfo_rows = (
        _bronze_df
        .filter(F.col("l1element") == "gameInfo")
        .select(
            F.col("l0payload").cast("string").alias("l0"),
            F.col("payload").cast("string").alias("p"),
            "ingested_from",
            "ingested_at",
        )
        .collect()
    )

    # ── teams rows ───────────────────────────────────────────────────────────
    teams_rows = (
        _bronze_df
        .filter(F.col("l1element") == "teams")
        .select(
            F.col("l0payload").cast("string").alias("l0"),
            F.col("payload").cast("string").alias("p"),
            "ingested_from",
            "ingested_at",
        )
        .collect()
    )

    print(f"[matchdata post_run_fn] {len(gameinfo_rows)} gameInfo rows, {len(teams_rows)} teams rows")

    broadcaster_records = []
    team_player_records = []

    # ── broadcaster records from gameInfo ────────────────────────────────────
    for row in gameinfo_rows:
        l0 = _json.loads(row.l0) if row.l0 else {}
        p  = _json.loads(row.p)  if row.p  else {}
        if isinstance(p, list):
            p = p[0] if p else {}

        game_id        = l0.get("gameId")
        competition_id = l0.get("competitionId")
        season_id      = l0.get("seasonId")
        ingested_from  = row.ingested_from
        ingested_at    = str(row.ingested_at)

        bi          = p.get("broadcastInfo") or l0.get("broadcastInfo") or {}
        regions_raw = (bi.get("regions") or {}).get("region", [])
        if isinstance(regions_raw, dict):
            regions_raw = [regions_raw]

        for region in regions_raw:
            if not isinstance(region, dict):
                continue
            region_id   = region.get("regionid")
            region_name = region.get("regionName")

            bcs_raw      = region.get("broadcasters") or {}
            broadcasters = bcs_raw.get("broadcaster", []) if isinstance(bcs_raw, dict) else []
            if isinstance(broadcasters, dict):
                broadcasters = [broadcasters]

            for bc in broadcasters:
                if not isinstance(bc, dict):
                    continue
                broadcaster_records.append({
                    "game_id":        game_id,
                    "competition_id": competition_id,
                    "season_id":      season_id,
                    "region_id":      region_id,
                    "region_name":    region_name,
                    "broadcaster_id": bc.get("broadcasterID"),
                    "broadcaster":    bc.get("broadcaster"),
                    "ingested_from":  ingested_from,
                    "ingested_at":    ingested_at,
                })

    # ── team player + playerStats records from teams ─────────────────────────
    for row in teams_rows:
        l0 = _json.loads(row.l0) if row.l0 else {}
        p  = _json.loads(row.p)  if row.p  else {}
        if isinstance(p, list):
            p = p[0] if p else {}
        if not isinstance(p, dict):
            continue

        ingested_from  = row.ingested_from
        ingested_at    = str(row.ingested_at)
        game_id        = l0.get("gameId")
        competition_id = l0.get("competitionId")
        season_id      = l0.get("seasonId")

        teams_match = p.get("teamsMatch", [])
        if isinstance(teams_match, dict):
            teams_match = [teams_match]

        for team in teams_match:
            if not isinstance(team, dict):
                continue
            team_id     = team.get("teamId")
            team_lineup = team.get("teamLineup") or {}
            if not isinstance(team_lineup, dict):
                continue

            players = team_lineup.get("teamPlayer", [])
            if isinstance(players, dict):
                players = [players]

            for player in players:
                if not isinstance(player, dict):
                    continue
                player_row = {k: v for k, v in player.items() if not isinstance(v, (dict, list))}
                for sk, sv in (player.get("playerStats") or {}).items():
                    player_row[f"player_stats_{sk}"] = sv
                team_player_records.append({
                    **player_row,
                    "team_id":        team_id,
                    "game_id":        game_id,
                    "competition_id": competition_id,
                    "season_id":      season_id,
                    "ingested_from":  ingested_from,
                    "ingested_at":    ingested_at,
                })

    # ── write helper ─────────────────────────────────────────────────────────
    def _write(records, table_name):
        if not records:
            print(f"[matchdata post_run_fn] No records for {table_name} — skipping")
            return
        _keys   = list(dict.fromkeys(k for r in records for k in r))
        _schema = _ST([_SF(k, _S(), True) for k in _keys])
        _rows   = [{k: (str(r[k]) if k in r and r[k] is not None else None) for k in _keys} for r in records]
        (
            spark.createDataFrame(_rows, schema=_schema)
            .write.format("delta")
            .mode(write_mode)
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        print(f"[matchdata post_run_fn] {len(_rows)} rows -> {table_name} (mode={write_mode})")

    _write(broadcaster_records, f"{target_prefix}_broadcaster")
    _write(team_player_records, f"{target_prefix}_teams_team_player")


MATCHDATA_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.match_stats",
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.match_stats",

    "source_filter_template": "ingested_from LIKE 'match_stats_{competition_id}_{season}%'",

    "exclude_fields": {
        "injuriesAndSuspensions",
        "radioNetwork",
        "broadcastInfo",    # handled by post_run_fn
        "teamPlayer",       # handled by post_run_fn — nested array inside teamLineup
    },

    "flatten_structs": {"score": "score_"},

    "force_array_fields": {"teamCoach"},

    "elements": {
        "gameInfo": "gameinfo",
        "teams":    "teams",
    },

    "dedup_keys_map": {
        "gameinfo":   ["ingested_from"],
        "teams":      ["game_id", "ingested_from"],
        "teamsMatch": ["game_id", "team_id"],
        "teamcoach":  ["coach_id"],
        "insouts":    ["player_id", "ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    "post_run_fn": _create_matchdata_tables,
}