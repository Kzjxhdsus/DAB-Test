"""
NRL Match Stats and Events API — Flattening Config
Source : nrl_datalakehouse_prd.bronze_stats
Bronze : two l1elements, both in standard mode (l0payload is NULL).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"gameStats"
  l0payload : NULL (standard mode — framework uses payload directly)
  payload   : single OBJECT —
    {competitionId, gameId, gameInfo{...}, gameState, roundId, seasonId,
     teams: {teamsMatch: [{teamId, score{}, teamLineup{teamPlayer[]},
                           teamStats{}, teamCoaches{}}, ...]},
     broadcastInfo: {regions: {region: [...]}}}
  post_run_fn creates:
    _gamestats_gameinfo      scalar game-level fields from payload root
    _gamestats_team          per-team stats (score + teamStats inlined)
    _gamestats_team_player   per-player stats (playerStats inlined)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"eventFlow"
  l0payload : NULL (standard mode — framework uses payload directly)
  payload   : single OBJECT with *Flow-suffixed keys plus top-level scalars:
    competitionId, gameId, roundId, seasonId          <- scalar keys
    scoreFlow:      {scoreEvent: {teams: {team: [{...statScoringEvent[]...}]}}}
    possessionFlow: {possessionEvent: {statPossessionEvent: [...]}}
    penaltyFlow:    {penaltyEvent:    {statPenaltyEvent:    [...]}}
    errorFlow:      {errorEvent:      {statErrorEvent:      [...]}}
    scrumFlow:      {scrumEvent:      {statScrumEvent:      [...]}}
    interchangeFlow:{team: [{interchanges: [...], isHomeTeam, teamId, teamName}]}
    disciplineFlow: {team: [{events:      [...], isHomeTeam, teamId, teamName}]}
    commentaryFlow: {event: [...]}
    colourEventsFlow:{event: [...]}
  post_run_fn creates:
    _eventflow_score_event_team                  scoring totals per team
    _eventflow_score_event_team_statscoringevent individual scoring events
    _eventflow_possession_event
    _eventflow_penalty_event
    _eventflow_error_event
    _eventflow_scrum_event
    _eventflow_interchange                       on/off records (interchangeFlow)
    _eventflow_discipline                        sin-bin / send-off records
    _eventflow_commentary_event                  commentary events
    _eventflow_colour_event                      colour events
"""

import json as _json
import re   as _re
from pyspark.sql import functions as F


def _to_list(val):
    """Normalise a value that may be a list, dict (single-element), or absent."""
    if isinstance(val, list):
        return val
    if isinstance(val, dict):
        return [val]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# post_run_fn
# ─────────────────────────────────────────────────────────────────────────────
def _create_matchstatsandevents_tables(config, spark):
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

    # ── accumulators ──────────────────────────────────────────────────────────
    # gameStats
    gameinfo_records        = []
    gamestats_team_records  = []
    team_player_records     = []
    # eventFlow
    scoreevent_team_records  = []
    statscoringevent_records = []
    possession_records       = []
    penalty_records          = []
    error_records            = []
    scrum_records            = []
    interchange_records      = []
    discipline_records       = []
    commentary_records       = []
    colour_records           = []

    for row in rows:
        l0      = _json.loads(row.l0) if row.l0 else {}
        payload = _json.loads(row.p)  if row.p  else []

        game_id = l0.get("gameId")
        if game_id is None:
            if isinstance(payload, dict):
                game_id = payload.get("gameId")
            elif isinstance(payload, list) and payload and isinstance(payload[0], dict):
                game_id = payload[0].get("gameId")

        ingested_from = row.ingested_from
        ingested_at   = str(row.ingested_at)
        base = {"game_id": game_id, "ingested_from": ingested_from, "ingested_at": ingested_at}

        # ── gameStats ─────────────────────────────────────────────────────────
        if row.l1element == "gameStats":
            if isinstance(payload, list):
                payload = payload[0] if payload and isinstance(payload[0], dict) else {}
            if not isinstance(payload, dict):
                continue

            gi_row = {k: v for k, v in payload.items() if not isinstance(v, (dict, list))}
            gameinfo_records.append({**gi_row, **base})

            teams_obj = payload.get("teams") or {}
            if not isinstance(teams_obj, dict):
                teams_obj = {}

            for team in _to_list(teams_obj.get("teamsMatch", [])):
                if not isinstance(team, dict):
                    continue
                team_id = team.get("teamId")

                team_row = {k: v for k, v in team.items() if not isinstance(v, (dict, list))}
                for sk, sv in (team.get("score") or {}).items():
                    team_row[f"score_{sk}"] = sv
                for sk, sv in (team.get("teamStats") or {}).items():
                    team_row[f"team_stats_{sk}"] = sv
                gamestats_team_records.append({**team_row, **base})

                team_lineup = team.get("teamLineup") or {}
                if isinstance(team_lineup, dict):
                    for player in _to_list(team_lineup.get("teamPlayer", [])):
                        if not isinstance(player, dict):
                            continue
                        player_row = {k: v for k, v in player.items() if not isinstance(v, (dict, list))}
                        for sk, sv in (player.get("playerStats") or {}).items():
                            player_row[f"player_stats_{sk}"] = sv
                        team_player_records.append({**player_row, "team_id": team_id, **base})

        # ── eventFlow ─────────────────────────────────────────────────────────
        elif row.l1element == "eventFlow":
            if isinstance(payload, list):
                payload = payload[0] if payload and isinstance(payload[0], dict) else {}
            if not isinstance(payload, dict):
                continue

            game_id        = payload.get("gameId") or game_id
            competition_id = payload.get("competitionId")
            round_id       = payload.get("roundId")
            season_id      = payload.get("seasonId")
            base = {
                "game_id":        game_id,
                "competition_id": competition_id,
                "round_id":       round_id,
                "season_id":      season_id,
                "ingested_from":  ingested_from,
                "ingested_at":    ingested_at,
            }

            for flow_key, flow_data in payload.items():
                if not isinstance(flow_data, dict):
                    continue

                if flow_key == "scoreFlow":
                    score_event = flow_data.get("scoreEvent") or {}
                    if not isinstance(score_event, dict):
                        continue
                    teams_node = score_event.get("teams") or {}
                    for team in _to_list(teams_node.get("team", [])) if isinstance(teams_node, dict) else []:
                        if not isinstance(team, dict):
                            continue
                        team_id      = team.get("teamId")
                        is_home_team = team.get("isHomeTeam")
                        team_row = {k: v for k, v in team.items() if not isinstance(v, (dict, list))}
                        scoreevent_team_records.append({**team_row, **base})
                        for evt in _to_list(team.get("statScoringEvent", [])):
                            if not isinstance(evt, dict):
                                continue
                            evt_row = {k: v for k, v in evt.items() if not isinstance(v, (dict, list))}
                            statscoringevent_records.append({
                                **evt_row,
                                "team_id":      team_id,
                                "is_home_team": is_home_team,
                                **base,
                            })

                elif flow_key == "possessionFlow":
                    possession_event = flow_data.get("possessionEvent") or {}
                    if not isinstance(possession_event, dict):
                        continue
                    for evt in _to_list(possession_event.get("statPossessionEvent", [])):
                        if not isinstance(evt, dict):
                            continue
                        evt_row = {k: v for k, v in evt.items() if not isinstance(v, (dict, list))}
                        possession_records.append({**evt_row, **base})

                elif flow_key == "penaltyFlow":
                    penalty_event = flow_data.get("penaltyEvent") or {}
                    if not isinstance(penalty_event, dict):
                        continue
                    for evt in _to_list(penalty_event.get("statPenaltyEvent", [])):
                        if not isinstance(evt, dict):
                            continue
                        evt_row = {k: v for k, v in evt.items() if not isinstance(v, (dict, list))}
                        penalty_records.append({**evt_row, **base})

                elif flow_key == "errorFlow":
                    error_event = flow_data.get("errorEvent") or {}
                    if not isinstance(error_event, dict):
                        continue
                    for evt in _to_list(error_event.get("statErrorEvent", [])):
                        if not isinstance(evt, dict):
                            continue
                        evt_row = {k: v for k, v in evt.items() if not isinstance(v, (dict, list))}
                        error_records.append({**evt_row, **base})

                elif flow_key == "scrumFlow":
                    scrum_event = flow_data.get("scrumEvent") or {}
                    if not isinstance(scrum_event, dict):
                        continue
                    for evt in _to_list(scrum_event.get("statScrumEvent", [])):
                        if not isinstance(evt, dict):
                            continue
                        evt_row = {k: v for k, v in evt.items() if not isinstance(v, (dict, list))}
                        scrum_records.append({**evt_row, **base})

                elif flow_key == "interchangeFlow":
                    for team_dict in _to_list(flow_data.get("team", [])):
                        if not isinstance(team_dict, dict):
                            continue
                        team_id = team_dict.get("teamId")
                        is_home = team_dict.get("isHomeTeam")
                        team_fk = {
                            "game_id":        game_id,
                            "competition_id": competition_id,
                            "round_id":       round_id,
                            "season_id":      season_id,
                            "team_id":        team_id,
                            "is_home_team":   is_home,
                            "ingested_from":  ingested_from,
                            "ingested_at":    ingested_at,
                        }
                        for ix in _to_list(team_dict.get("interchanges", [])):
                            if not isinstance(ix, dict):
                                continue
                            ix_row = {k: v for k, v in ix.items() if not isinstance(v, (dict, list))}
                            for prefix, node in (("on", ix.get("on")), ("off", ix.get("off"))):
                                if isinstance(node, dict):
                                    for k, v in node.items():
                                        ix_row[f"{prefix}_{k}"] = v
                            interchange_records.append({**ix_row, **team_fk})

                elif flow_key == "disciplineFlow":
                    for team_dict in _to_list(flow_data.get("team", [])):
                        if not isinstance(team_dict, dict):
                            continue
                        team_id = team_dict.get("teamId")
                        is_home = team_dict.get("isHomeTeam")
                        team_fk = {
                            "game_id":        game_id,
                            "competition_id": competition_id,
                            "round_id":       round_id,
                            "season_id":      season_id,
                            "team_id":        team_id,
                            "is_home_team":   is_home,
                            "ingested_from":  ingested_from,
                            "ingested_at":    ingested_at,
                        }
                        for ev in _to_list(team_dict.get("events", [])):
                            if not isinstance(ev, dict):
                                continue
                            ev_row = {k: v for k, v in ev.items() if not isinstance(v, (dict, list))}
                            discipline_records.append({**ev_row, **team_fk})

                elif flow_key == "commentaryFlow":
                    for evt in _to_list(flow_data.get("event", [])):
                        if not isinstance(evt, dict):
                            continue
                        evt_row = {k: v for k, v in evt.items() if not isinstance(v, (dict, list))}
                        if evt_row:
                            commentary_records.append({**evt_row, **base})

                elif flow_key == "colourEventsFlow":
                    for evt in _to_list(flow_data.get("event", [])):
                        if not isinstance(evt, dict):
                            continue
                        evt_row = {k: v for k, v in evt.items() if not isinstance(v, (dict, list))}
                        if evt_row:
                            colour_records.append({**evt_row, **base})

    # ── Write tables ──────────────────────────────────────────────────────────
    write_mode = config.get("write_mode", "overwrite")

    def _write(records, table_name):
        if not records:
            print(f"[matchstatsandevents] No records for {table_name} — skipping")
            return
        from pyspark.sql.types import StructType as _ST, StructField as _SF, StringType as _S
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
        print(f"[matchstatsandevents] {len(_rows)} rows -> {table_name} (mode={write_mode})")

    _write(gameinfo_records,         f"{target_prefix}_gamestats_gameinfo")
    _write(gamestats_team_records,   f"{target_prefix}_gamestats_team")
    _write(team_player_records,      f"{target_prefix}_gamestats_team_player")
    _write(scoreevent_team_records,  f"{target_prefix}_eventflow_score_event_team")
    _write(statscoringevent_records, f"{target_prefix}_eventflow_score_event_team_statscoringevent")
    _write(possession_records,       f"{target_prefix}_eventflow_possession_event")
    _write(penalty_records,          f"{target_prefix}_eventflow_penalty_event")
    _write(error_records,            f"{target_prefix}_eventflow_error_event")
    _write(scrum_records,            f"{target_prefix}_eventflow_scrum_event")
    _write(interchange_records,      f"{target_prefix}_eventflow_interchange")
    _write(discipline_records,       f"{target_prefix}_eventflow_discipline")
    _write(commentary_records,       f"{target_prefix}_eventflow_commentary_event")
    _write(colour_records,           f"{target_prefix}_eventflow_colour_event")


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
MATCHSTATSANDEVENTS_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.match_stats_and_events",
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.match_stats_and_events",

    "source_filter_template": "ingested_from LIKE 'match_stats_and_events_{competition_id}_{season}%'",

    "exclude_fields": {
        "_l1_array",
        "radioNetwork",
        "injuriesAndSuspensions",
        "insAndOuts",
        "teamsMatch",
        "statScoringEvent",
        "statPossessionEvent",
        "statPenaltyEvent",
        "statErrorEvent",
        "statScrumEvent",
        "team",
        "interchanges",
        "events",
        "event",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "gameStats": "gamestats",
        "eventFlow": "eventflow",
    },

    "dedup_keys_map": {
        "gamestats": ["game_id"],
        "eventflow": ["game_id"],
    },

    "sample_size":    200,
    "type_overrides": {},

    "post_run_fn": _create_matchstatsandevents_tables,
}