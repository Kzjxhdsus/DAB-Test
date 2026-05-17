"""
NRL Match Data By Round Breakdown API — Flattening Config
Source  : api/NRL/matchDataByRoundBreakdown/{competitionId}/{seasonId}/{roundId}/{gameId}
Bronze  : nrl_datalakehouse_qa.bronze_stats.match_data_by_round_breakdown
Mode    : l0payload is an OBJECT (<gameStatsPeriod> root attributes).
Elements: l1element = 'teams'    — array of teamsMatch objects
          l1element = 'gameInfo' — flat game metadata attributes

ingested_from pattern : match_data_by_round_breakdown_{competitionId}_{seasonId}_{roundId}_{gameId}
e.g. match_data_by_round_breakdown_111_2026_1_20261110110

l0payload (object — <gameStatsPeriod> root attributes):
  { competitionId, gameId, roundId, seasonId, gameState, gameStateId, roundName }

l1element = 'gameInfo' payload — flat attrs (broadcastInfo excluded as nested dict):
  { assistantReferee1Id, assistantReferee2Id, crowd, gameDate, gameHashTag,
    gameNumber, gameSeconds, gameMinutes, gameTime,
    groundConditionId, groundConditionName, referee1Id, referee1Name,
    seniorReviewOfficialId, seniorReviewOfficialName,
    startTime, startTimeUTC, venueId, venueName, city,
    weatherId, weatherName, venueLatitude, venueLongitude,
    broadcastInfo: { ... } }

l1element = 'teams' payload — teamsMatch array:
  teamsMatch {
    isHomeTeam, team1stHalfScore, team2ndHalfScore, teamAbbr, teamFinalScore,
    teamFullTimeScore, teamHalfTimeScore, teamId, teamName, teamNickName,
    teamHexColour, teamHexColour2,
    teamHeadToHeadOdds, teamHeadToHeadStatus, teamHeadToHeadOutcomeID,
    score { conversions, points, fieldGoals, penaltyGoals, tries, allGoals },
    teamLineup { teamLineUpStatusID, teamLineUpStatus,
      injuriesAndSuspensions { ... },
      insAndOuts { ... },
      teamPlayer [ {
        playerFirstName, playerId, playerLastName, playerName,
        playerPosition, playerPositionId, onTheField, playerTookTheField,
        shirtNum, isCaptain, isViceCaptain, playerFirstInitial, playerPositionAbbrev,
        playerStats {
          matchStats { periodId="0", [~200 stat attrs] },
          periodStats [ { periodId, [stat attrs] }, ... ]
        }
      } ]
    },
    teamStats {
      matchStats { periodId="0", [team stat attrs] },
      periodStats [ { periodId, [stat attrs] }, ... ]
    },
    teamCoaches {
      teamCoach [ { coachId, coachName, coachRoleId, coachRole } ]
    }
  }

Output tables (target_prefix = nrl_datalakehouse_qa.bronze_stats_flatten.nrl_round_match_data):
  nrl_round_match_data                     <- Level 0   Game root (one row per game)
  nrl_round_match_data_game_info           <- Level 0b  Game info (one row per game)
  nrl_round_match_data_team                <- Level 1   Team match context (one row per team per game)
  nrl_round_match_data_team_score          <- Level 1b  Team score breakdown (one row per team per game)
  nrl_round_match_data_team_lineup         <- Level 2   Player lineup entry (one row per player per team)
  nrl_round_match_data_player_stats        <- Level 3a  Player full-match stats (periodId=0)
  nrl_round_match_data_player_period_stats <- Level 3b  Player period stats (one row per player per period)
  nrl_round_match_data_team_stats          <- Level 4a  Team full-match stats (periodId=0)
  nrl_round_match_data_team_period_stats   <- Level 4b  Team period stats (one row per team per period)
  nrl_round_match_data_team_coach          <- Level 5   Team coach (one row per coach per team per game)

NOTE: all tables are created by post_run_fn.
      FK columns propagated: competition_id, season_id, round_id, game_id
      (+ team_id from Level 1+, + player_id from Level 3+).
      Names and descriptive attrs live only at their own level — not propagated as FKs.
"""

import json as _json
import re   as _re
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_snake(name: str) -> str:
    """camelCase / PascalCase → snake_case."""
    s = _re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = _re.sub(r'([a-z\d])([A-Z])',      r'\1_\2', s)
    return s.lower()


def _to_list(val):
    """Normalise a value that may be a list, a dict (single element), or absent."""
    if isinstance(val, list):
        return val
    if isinstance(val, dict):
        return [val]
    return []


def _scalars(d: dict) -> dict:
    """Return only non-dict / non-list values, keys converted to snake_case."""
    return {_to_snake(k): v for k, v in d.items() if not isinstance(v, (dict, list))}


# ---------------------------------------------------------------------------
# Writer helper
# ---------------------------------------------------------------------------
def _write_records(records, table_name, spark, write_mode="overwrite"):
    """Build an explicit all-STRING schema and write records to a Delta table."""
    if not records:
        print(f"[matchdatabyroundbreakdown] No records for {table_name} — skipping")
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
    print(f"[matchdatabyroundbreakdown] {len(rows)} rows → {table_name} (mode={write_mode})")


# ---------------------------------------------------------------------------
# post_run_fn
# ---------------------------------------------------------------------------
def _create_match_data_by_round_breakdown_tables(config, spark):
    """
    Creates ten doc-aligned tables from the matchDataByRoundBreakdown bronze data.

    nrl_round_match_data                     — one row per game (root context)
    nrl_round_match_data_game_info           — one row per game (game info flat attrs)
    nrl_round_match_data_team                — one row per team per game (team match context)
    nrl_round_match_data_team_score          — one row per team per game (score breakdown)
    nrl_round_match_data_team_lineup         — one row per player per team (player identity + lineup flags)
    nrl_round_match_data_player_stats        — one row per player (periodId=0 full-match stats)
    nrl_round_match_data_player_period_stats — one row per player per period
    nrl_round_match_data_team_stats          — one row per team (periodId=0 full-match stats)
    nrl_round_match_data_team_period_stats   — one row per team per period
    nrl_round_match_data_team_coach          — one row per coach per team per game
    """
    bronze = config["bronze_table"]
    prefix = config["target_prefix"]
    bf     = config.get("bronze_filter")

    bronze_df = spark.table(bronze)
    if bf:
        bronze_df = bronze_df.filter(bf)

    rows = (
        bronze_df
        .filter(F.col("l1element").isin("teams", "gameInfo"))
        .select(
            "ingested_from",
            "ingested_at",
            "l1element",
            F.col("payload").cast("string").alias("p"),
            F.col("l0payload").cast("string").alias("l0"),
        )
        .collect()
    )

    root_records          = []
    game_info_records     = []
    team_records          = []
    team_score_records    = []
    team_lineup_records   = []
    player_stats_records  = []
    player_period_records = []
    team_stats_records    = []
    team_period_records   = []
    team_coach_records    = []

    seen_root      = set()
    seen_game_info = set()

    for row in rows:
        # Parse context: match_data_by_round_breakdown_{competitionId}_{seasonId}_{roundId}_{gameNum}_{gameId}
        # e.g. match_data_by_round_breakdown_111_2026_1_2_20261110120
        #       parts: 0=match 1=data 2=by 3=round 4=breakdown 5=compId 6=seasonId 7=roundId 8=gameNum 9=gameId
        parts          = row.ingested_from.split("_")
        competition_id = parts[5] if len(parts) > 5 else None
        season_id      = parts[6] if len(parts) > 6 else None
        round_id       = parts[7] if len(parts) > 7 else None
        game_num       = parts[8] if len(parts) > 8 else None   # game number within round
        game_id        = parts[9] if len(parts) > 9 else None

        # Extract root attrs from l0payload
        competition_name = None
        game_state       = None
        game_state_id    = None
        round_name       = None
        if row.l0:
            try:
                l0 = _json.loads(row.l0)
                if isinstance(l0, dict):
                    competition_name = l0.get("competitionName")
                    game_state       = l0.get("gameState")
                    game_state_id    = l0.get("gameStateId")
                    round_name       = l0.get("roundName")
            except Exception:
                pass

        ctx = {
            "ingested_from":     row.ingested_from,
            "ingested_at":       str(row.ingested_at),
            "competition_id":    competition_id,
            "season_id":         season_id,
            "round_id":          round_id,
            "game_num":          game_num,
            "game_id":           game_id,
            "source_schema":     "bronze_stats",
            "source_table_name": "match_data_by_round_breakdown",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Level 0: Root row — one per game ──────────────────────────────────
        root_key = (competition_id, season_id, round_id, game_id)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name":  competition_name,
                "game_state":        game_state,
                "game_state_id":     game_state_id,
                "round_name":        round_name,
                "target_table_name": "nrl_round_match_data",
            })

        # ── Level 0b: Game info — own l1element row ───────────────────────────
        if row.l1element == "gameInfo":
            game_info_key = root_key
            if game_info_key not in seen_game_info:
                seen_game_info.add(game_info_key)
                payload = _json.loads(row.p) if row.p else {}
                if isinstance(payload, dict):
                    game_info_records.append({
                        **_scalars(payload),   # excludes nested broadcastInfo dict
                        **ctx,
                        "target_table_name": "nrl_round_match_data_game_info",
                    })
            continue   # nothing else to process for this l1element

        # ── Teams (l1element == 'teams') ──────────────────────────────────────
        payload = _json.loads(row.p) if row.p else []
        if isinstance(payload, dict):
            teams = _to_list(payload.get("teamsMatch") or payload)
        else:
            teams = _to_list(payload)

        for team in teams:
            if not isinstance(team, dict):
                continue

            team_id  = team.get("teamId")
            team_ctx = {**ctx, "team_id": team_id}

            # ── Level 1: Team match context ───────────────────────────────────
            # _scalars excludes nested score/teamLineup/teamStats/teamCoaches dicts
            team_records.append({
                **_scalars(team),
                **team_ctx,            # FKs overwrite any same-named key from _scalars
                "target_table_name": "nrl_round_match_data_team",
            })

            # ── Level 1b: Team score breakdown ────────────────────────────────
            score_node = team.get("score") or {}
            if isinstance(score_node, dict) and score_node:
                team_score_records.append({
                    **_scalars(score_node),
                    **team_ctx,
                    "target_table_name": "nrl_round_match_data_team_score",
                })

            # ── Level 2: Player lineup — one row per player ───────────────────
            lineup_node = team.get("teamLineup") or {}
            if isinstance(lineup_node, dict):
                lineup_status_id = lineup_node.get("teamLineUpStatusID")
                lineup_status    = lineup_node.get("teamLineUpStatus")

                for player in _to_list(lineup_node.get("teamPlayer")):
                    if not isinstance(player, dict):
                        continue

                    player_id  = player.get("playerId")
                    player_ctx = {**team_ctx, "player_id": player_id}

                    # Player identity + lineup metadata
                    # _scalars excludes nested playerStats dict
                    team_lineup_records.append({
                        **_scalars(player),
                        "lineup_status_id": lineup_status_id,
                        "lineup_status":    lineup_status,
                        **player_ctx,
                        "target_table_name": "nrl_round_match_data_team_lineup",
                    })

                    # ── Level 3a/3b: Player stats ──────────────────────────────
                    player_stats_node = player.get("playerStats") or {}
                    if isinstance(player_stats_node, dict):

                        # matchStats — periodId=0 (full match)
                        match_stats = player_stats_node.get("matchStats")
                        if isinstance(match_stats, dict) and match_stats:
                            player_stats_records.append({
                                **_scalars(match_stats),
                                **player_ctx,
                                "target_table_name": "nrl_round_match_data_player_stats",
                            })

                        # periodStats — periodId=1, 2, …
                        for ps in _to_list(player_stats_node.get("periodStats")):
                            if not isinstance(ps, dict):
                                continue
                            player_period_records.append({
                                **_scalars(ps),
                                **player_ctx,
                                "target_table_name": "nrl_round_match_data_player_period_stats",
                            })

            # ── Level 4a/4b: Team stats ───────────────────────────────────────
            team_stats_node = team.get("teamStats") or {}
            if isinstance(team_stats_node, dict):

                # matchStats — periodId=0 (full match)
                team_match_stats = team_stats_node.get("matchStats")
                if isinstance(team_match_stats, dict) and team_match_stats:
                    team_stats_records.append({
                        **_scalars(team_match_stats),
                        **team_ctx,
                        "target_table_name": "nrl_round_match_data_team_stats",
                    })

                # periodStats — periodId=1, 2, …
                for tps in _to_list(team_stats_node.get("periodStats")):
                    if not isinstance(tps, dict):
                        continue
                    team_period_records.append({
                        **_scalars(tps),
                        **team_ctx,
                        "target_table_name": "nrl_round_match_data_team_period_stats",
                    })

            # ── Level 5: Team coaches ─────────────────────────────────────────
            coaches_node = team.get("teamCoaches") or {}
            if isinstance(coaches_node, dict):
                for coach in _to_list(coaches_node.get("teamCoach")):
                    if not isinstance(coach, dict):
                        continue
                    team_coach_records.append({
                        **_scalars(coach),
                        **team_ctx,
                        "target_table_name": "nrl_round_match_data_team_coach",
                    })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,          f"{prefix}",                      spark, write_mode)
    _write_records(game_info_records,     f"{prefix}_game_info",            spark, write_mode)
    _write_records(team_records,          f"{prefix}_team",                 spark, write_mode)
    _write_records(team_score_records,    f"{prefix}_team_score",           spark, write_mode)
    _write_records(team_lineup_records,   f"{prefix}_team_lineup",          spark, write_mode)
    _write_records(player_stats_records,  f"{prefix}_player_stats",         spark, write_mode)
    _write_records(player_period_records, f"{prefix}_player_period_stats",  spark, write_mode)
    _write_records(team_stats_records,    f"{prefix}_team_stats",           spark, write_mode)
    _write_records(team_period_records,   f"{prefix}_team_period_stats",    spark, write_mode)
    _write_records(team_coach_records,    f"{prefix}_team_coach",           spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
MATCHDATABYROUNDBREAKDOWN_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_qa.bronze_stats.match_data_by_round_breakdown",
    "target_prefix": "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_round_match_data",

    # Always append so historical loads accumulate correctly.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'match_data_by_round_breakdown_{competition_id}_{season}%'",

    # All nested structures handled by post_run_fn.
    "exclude_fields": {
        "teamsMatch",
        "score",
        "teamLineup",
        "teamPlayer",
        "playerStats",
        "matchStats",
        "periodStats",
        "teamStats",
        "teamCoaches",
        "teamCoach",
        "broadcastInfo",
        "injuriesAndSuspensions",
        "insAndOuts",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "teams":    "teams",
        "gameInfo": "gameInfo",
    },

    "dedup_keys_map": {
        "teams":    ["ingested_from"],
        "gameInfo": ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Suppress framework-generated tables; all tables owned by post_run_fn.
    "exclude_root_aliases": {"teams", "gameInfo"},

    # Creates: nrl_round_match_data,
    #          nrl_round_match_data_game_info,
    #          nrl_round_match_data_team,
    #          nrl_round_match_data_team_score,
    #          nrl_round_match_data_team_lineup,
    #          nrl_round_match_data_player_stats,
    #          nrl_round_match_data_player_period_stats,
    #          nrl_round_match_data_team_stats,
    #          nrl_round_match_data_team_period_stats,
    #          nrl_round_match_data_team_coach
    "post_run_fn": _create_match_data_by_round_breakdown_tables,
}
