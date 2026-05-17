"""
NRL Match Snapshot Data API — Flattening Config
Source  : api/NRL/snapshotStats/{competitionId}/{seasonId}/{gameId}
Bronze  : nrl_datalakehouse_prd.bronze_stats.matchsnapshotdata
Mode    : l0payload is an OBJECT (snapshotStats root attrs);
          payload per l1element is element-specific.
Elements: l1element = 'gameInfo' | 'teams' | 'interchangeFlow'
                    | 'injuriesAndSuspensions' | 'disciplineFlow'

ingested_from pattern : matchSnapshotData_{competitionId}_{seasonId}_{gameId}
e.g. matchSnapshotData_111_2026_20261110110

l0payload (object — <snapshotStats> root attributes):
  { competitionId, competitionName, gameId, gameState, gameStateId,
    roundId, seasonId }

l1element payloads:
  gameInfo              — scalar attrs merged into root row:
                          { assistantReferee1Id/Name, assistantReferee2Id/Name,
                            crowd, gameDate, gameHashTag, gameNumber,
                            gameSeconds, gameMinutes, gameTime,
                            groundConditionId/Name, numberOfPeriods,
                            referee1Id/Name, seniorReviewOfficialId/Name,
                            startTime, startTimeUTC, venueId, venueName,
                            weatherId, weatherName }

  teams                 — {"teamsMatch": [ team, ... ]}
                          Each team: isHomeTeam, team1stHalfScore, team2ndHalfScore,
                            teamAbbr, teamFinalScore, teamFullTimeScore,
                            teamHalfTimeScore, teamId, teamName, teamNickName
                          Children:
                            score      { conversions, points, fieldGoals,
                                         penaltyGoals, tries, allGoals }
                            teamLineup { teamLineUpStatus, teamLineUpStatusID,
                                         teamPlayer [ { playerFirstName,
                                           playerId, playerLastName, playerName,
                                           playerPosition, playerPositionId,
                                           playerTookTheField, shirtNum,
                                           isCaptain, isViceCaptain,
                                           positionAbbrev (text child),
                                           playerStats { ... } } ] }
                            teamStats  { allGoals, tries, tackles, ... }

  interchangeFlow       — [ { teamId, teamName, isHomeTeam,
                               interchanges [ { interchangeNumber, ..., gameTime,
                                 on  { playerId, shirtNum, playerName, statID },
                                 off { playerId, shirtNum, playerName, statID } } ] } ]

  injuriesAndSuspensions— [] (typically empty)

  disciplineFlow        — [ { teamId, teamName, isHomeTeam,
                               events [ { disciplineNumber, gameStateId, gameState,
                                          gameTime, gameSeconds, gameMinutes,
                                          yellow, red, playerId, playerName,
                                          utcTime } ] } ]

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.nrl_match_snapshot):
  nrl_match_snapshot                       ← root (l0payload + gameInfo merged)
  nrl_match_snapshot_team                  ← per team (score + teamLineup scalars
                                             + teamStats inlined)
  nrl_match_snapshot_team_player           ← per player per team
                                             (positionAbbrev + playerStats inlined)
  nrl_match_snapshot_interchange           ← player interchange events
  nrl_match_snapshot_injuries_suspensions  ← injury & suspension entries
  nrl_match_snapshot_discipline            ← discipline / sin-bin / send-off events

NOTE: all six tables are created by post_run_fn.
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
# post_run_fn
# ---------------------------------------------------------------------------
def _create_match_snapshot_tables(config, spark):
    """
    Creates six doc-aligned tables from the matchsnapshotdata bronze data.

    nrl_match_snapshot                      — one row per game
                                              (l0payload snapshotStats attrs
                                               + gameInfo game details merged)
    nrl_match_snapshot_team                 — one row per team per game
    nrl_match_snapshot_team_player          — one row per player per team per game
    nrl_match_snapshot_interchange          — one row per interchange event
    nrl_match_snapshot_injuries_suspensions — one row per injury/suspension entry
    nrl_match_snapshot_discipline           — one row per discipline event
    """
    bronze        = config["bronze_table"]
    target_prefix = config["target_prefix"]

    _bronze_df = spark.table(bronze)
    _bf = config.get("bronze_filter")
    if _bf:
        _bronze_df = _bronze_df.filter(_bf)

    rows = (
        _bronze_df
        .select(
            F.col("l1element"),
            F.col("l0payload").cast("string").alias("l0"),
            F.col("payload").cast("string").alias("p"),
            "ingested_from",
            "ingested_at",
        )
        .collect()
    )

    # ── accumulators ─────────────────────────────────────────────────────────
    # Root uses a dict keyed by (comp, season, game) so fields from l0payload
    # and gameInfo are merged regardless of row order.
    root_data           = {}   # (competition_id, season_id, game_id) → dict
    team_records        = []
    team_player_records = []
    interchange_records = []
    injury_records      = []
    discipline_records  = []

    for row in rows:
        ingested_from = row.ingested_from
        ingested_at   = str(row.ingested_at)

        # Parse context: match_snapshot_{competitionId}_{seasonId}_..._{gameId}
        parts          = ingested_from.split("_")
        competition_id = parts[2] if len(parts) > 2 else None
        season_id      = parts[3] if len(parts) > 3 else None
        game_id        = parts[-1] if len(parts) > 3 else None

        ctx = {
            "ingested_from":     ingested_from,
            "ingested_at":       ingested_at,
            "competition_id":    competition_id,
            "season_id":         season_id,
            "game_id":           game_id,
            "source_schema":     "bronze_stats",
            "source_table_name": "match_snapshot",
            "target_schema":     "bronze_stats_flatten",
        }

        root_key = (competition_id, season_id, game_id)

        # ── Seed root record from l0payload (snapshotStats root attrs) ────────
        if row.l0:
            l0 = _json.loads(row.l0)
            if isinstance(l0, dict) and l0:
                if root_key not in root_data:
                    root_data[root_key] = {**ctx, "target_table_name": "nrl_match_snapshot"}
                root_data[root_key].update(_scalars(l0))

        payload = _json.loads(row.p) if row.p else {}

        # ── gameInfo — merge game details into root row ───────────────────────
        # Contains: referee names, crowd, venue, weather, times, ground condition
        if row.l1element == "gameInfo":
            gi = payload
            if isinstance(gi, list):
                gi = gi[0] if gi else {}
            if isinstance(gi, dict) and gi:
                if root_key not in root_data:
                    root_data[root_key] = {**ctx, "target_table_name": "nrl_match_snapshot"}
                root_data[root_key].update(_scalars(gi))

        # ── teams → teamsMatch[] ──────────────────────────────────────────────
        # Payload: {"teamsMatch": [{...}, {...}]}
        elif row.l1element == "teams":
            if isinstance(payload, dict):
                teams = _to_list(payload.get("teamsMatch") or payload)
            else:
                teams = _to_list(payload)

            for team in teams:
                if not isinstance(team, dict):
                    continue

                team_id   = team.get("teamId")
                team_name = team.get("teamName")
                is_home   = team.get("isHomeTeam")

                # ── team row ─────────────────────────────────────────────────
                team_row = _scalars(team)

                # Inline score.* with score_ prefix
                for sk, sv in (team.get("score") or {}).items():
                    team_row[f"score_{_to_snake(sk)}"] = sv

                # Inline teamLineup SCALAR fields only (status fields)
                team_lineup = team.get("teamLineup") or {}
                for lk, lv in _scalars(team_lineup).items():
                    team_row[lk] = lv

                # Inline teamStats.* with team_stats_ prefix
                for tk, tv in (team.get("teamStats") or {}).items():
                    team_row[f"team_stats_{_to_snake(tk)}"] = tv

                team_records.append({**team_row, **ctx})

                # ── team_player rows ─────────────────────────────────────────
                for player in _to_list(
                    team_lineup.get("teamPlayer") if isinstance(team_lineup, dict) else []
                ):
                    if not isinstance(player, dict):
                        continue

                    player_row = _scalars(player)

                    # positionAbbrev may be a text-only child element — some
                    # XML converters return it as a nested dict; unwrap if so.
                    pos_abbrev = player.get("positionAbbrev")
                    if isinstance(pos_abbrev, dict):
                        pos_abbrev = (
                            pos_abbrev.get("#text")
                            or pos_abbrev.get("_text")
                            or pos_abbrev.get("text")
                        )
                    if pos_abbrev is not None:
                        player_row["position_abbrev"] = pos_abbrev

                    # Inline playerStats.* with player_stats_ prefix
                    for ps_k, ps_v in (player.get("playerStats") or {}).items():
                        player_row[f"player_stats_{_to_snake(ps_k)}"] = ps_v

                    player_row["team_id"]      = team_id
                    player_row["team_name"]    = team_name
                    player_row["is_home_team"] = is_home

                    team_player_records.append({**player_row, **ctx})

        # ── interchangeFlow ───────────────────────────────────────────────────
        # Payload: [ { teamId, teamName, isHomeTeam,
        #              interchanges: [ { ..., on: {...}, off: {...} } ] } ]
        # XML converters may wrap as {"team": [...]} — unwrap if so.
        elif row.l1element == "interchangeFlow":
            if isinstance(payload, dict):
                payload = _to_list(payload.get("team") or payload)
            else:
                payload = _to_list(payload)
            for team in payload:
                if not isinstance(team, dict):
                    continue

                team_id   = team.get("teamId")
                team_name = team.get("teamName")
                is_home   = team.get("isHomeTeam")

                for interchange in _to_list(team.get("interchanges")):
                    if not isinstance(interchange, dict):
                        continue

                    event_row = _scalars(interchange)

                    for direction in ("on", "off"):
                        node = interchange.get(direction)
                        if isinstance(node, dict):
                            for k, v in node.items():
                                event_row[f"{direction}_{_to_snake(k)}"] = v

                    event_row["team_id"]      = team_id
                    event_row["team_name"]    = team_name
                    event_row["is_home_team"] = is_home

                    interchange_records.append({**event_row, **ctx})

        # ── injuriesAndSuspensions ────────────────────────────────────────────
        elif row.l1element == "injuriesAndSuspensions":
            for entry in _to_list(payload):
                if not isinstance(entry, dict):
                    continue
                injury_records.append({**_scalars(entry), **ctx})

        # ── disciplineFlow ────────────────────────────────────────────────────
        # Payload: [ { teamId, teamName, isHomeTeam,
        #              events: [ { disciplineNumber, gameState, yellow, red,
        #                          playerId, playerName, utcTime, ... } ] } ]
        # XML converters may wrap as {"team": [...]} — unwrap if so.
        elif row.l1element == "disciplineFlow":
            if isinstance(payload, dict):
                payload = _to_list(payload.get("team") or payload)
            else:
                payload = _to_list(payload)
            for team in payload:
                if not isinstance(team, dict):
                    continue

                team_id   = team.get("teamId")
                team_name = team.get("teamName")
                is_home   = team.get("isHomeTeam")

                for event in _to_list(team.get("events")):
                    if not isinstance(event, dict):
                        continue

                    event_row = _scalars(event)
                    event_row["team_id"]      = team_id
                    event_row["team_name"]    = team_name
                    event_row["is_home_team"] = is_home

                    discipline_records.append({**event_row, **ctx})

    # Flatten root_data dict → list
    root_records = list(root_data.values())

    # ── Write tables ──────────────────────────────────────────────────────────
    write_mode = config.get("write_mode", "overwrite")

    def _write(records, table_name):
        if not records:
            print(f"[matchsnapshotdata] No records for {table_name} — skipping")
            return
        from pyspark.sql.types import StructType as _ST, StructField as _SF, StringType as _S
        _keys   = list(dict.fromkeys(k for r in records for k in r))
        _schema = _ST([_SF(k, _S(), True) for k in _keys])
        _rows   = [
            {k: (str(r[k]) if k in r and r[k] is not None else None) for k in _keys}
            for r in records
        ]
        (
            spark.createDataFrame(_rows, schema=_schema)
            .write.format("delta")
            .mode(write_mode)
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        print(f"[matchsnapshotdata] {len(_rows)} rows -> {table_name} (mode={write_mode})")

    _write(root_records,        f"{target_prefix}")
    _write(team_records,        f"{target_prefix}_team")
    _write(team_player_records, f"{target_prefix}_team_player")
    _write(interchange_records, f"{target_prefix}_interchange")
    _write(injury_records,      f"{target_prefix}_injuries_suspensions")
    _write(discipline_records,  f"{target_prefix}_discipline")


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
MATCHSNAPSHOTDATA_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.match_snapshot",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_match_snapshot",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'match_snapshot_{competition_id}_{season}%'",

    "exclude_fields": {
        "teamsMatch",
        "team",
        "score",
        "teamLineup",
        "teamPlayer",
        "positionAbbrev",
        "playerStats",
        "teamStats",
        "interchanges",
        "on",
        "off",
        "events",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "gameInfo":               "game_info",
        "teams":                  "teams",
        "interchangeFlow":        "interchange_flow",
        "injuriesAndSuspensions": "injuries_and_suspensions",
        "disciplineFlow":         "discipline_flow",
    },

    "dedup_keys_map": {
        "game_info":                ["ingested_from"],
        "teams":                    ["ingested_from"],
        "interchange_flow":         ["ingested_from"],
        "injuries_and_suspensions": ["ingested_from"],
        "discipline_flow":          ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Suppress all framework-generated root tables; all tables owned by post_run_fn.
    "exclude_root_aliases": {
        "game_info",
        "teams",
        "interchange_flow",
        "injuries_and_suspensions",
        "discipline_flow",
    },

    # Creates: nrl_match_snapshot,
    #          nrl_match_snapshot_team,
    #          nrl_match_snapshot_team_player,
    #          nrl_match_snapshot_interchange,
    #          nrl_match_snapshot_injuries_suspensions,
    #          nrl_match_snapshot_discipline
    "post_run_fn": _create_match_snapshot_tables,
}
