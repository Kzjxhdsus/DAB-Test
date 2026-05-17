"""
NRL Event Flow API — Flattening Config
Source  : api/NRL/eventFlow/{competitionId}/{seasonId}/{gameId}
Bronze  : nrl_datalakehouse_prd.bronze_stats.eventflow
Mode    : l0payload is an OBJECT (eventFlow root attrs);
          payload per l1element is element-specific.
Elements: l1element = 'scoreFlow' | 'possessionFlow' | 'penaltyFlow' | 'errorFlow'
                    | 'scrumFlow'  | 'interchangeFlow' | 'commentaryFlow'
                    | 'colourEventsFlow' | 'disciplineFlow'

ingested_from pattern : eventFlow_{competitionId}_{seasonId}_{gameId}
e.g. eventFlow_111_2026_20261110110

l0payload (object — <eventFlow> root attributes):
  { competitionId, competitionName, gameId, roundId, seasonId }

l1element payloads:
  scoreFlow        — scoreEvent → teams → team[]
                     team attrs: isHomeTeam, teamId, teamName, teamAbbrv, points,
                       tries, conversions, fieldGoals, penaltyGoals,
                       period1Tries, period2Tries, extraTimeTries,
                       period1Conversions, period2Conversions, extraTimeConversions,
                       period1PenaltyGoals, period2PenaltyGoals, extraTimePenaltyGoals
                     statScoringEvent[]: gameState, gameStateId, gameTime,
                       gameSeconds, gameMinutes, playerId, playerName, scoreMethod,
                       statEvent, statId, statValue, vidRef, homeScore, awayScore,
                       commentary, tackleNum, utcTime

  possessionFlow   — possessionEvent → statPossessionEvent[]
                     attrs: teamId, teamName, isHomeTeam, statId, statValue,
                       gameTime, gameSeconds, gameMinutes, gameState, gameStateId,
                       setDescription, vidRef, tryScoringSet, penaltyScoringSet,
                       fieldGoalScoringSet, commentary, tackleNum, utcTime

  penaltyFlow      — penaltyEvent → statPenaltyEvent[]
                     attrs: teamId, teamName, playerId, isHomeTeam, statId, statValue,
                       gameTime, gameSeconds, gameMinutes, gameState, gameStateId,
                       penaltyDescription, vidRef, commentary, tackleNum, refId, utcTime

  errorFlow        — errorEvent → statErrorEvent[]
                     attrs: teamId, teamName, playerId, isHomeTeam, statId, statValue,
                       gameTime, gameSeconds, gameMinutes, gameState, gameStateId,
                       vidRef, commentary, tackleNum, utcTime

  scrumFlow        — scrumEvent → statScrumEvent[]
                     attrs: teamId, teamName, isHomeTeam, statId, statValue,
                       gameTime, gameSeconds, gameMinutes, gameState, gameStateId,
                       vidRef, commentary, utcTime

  interchangeFlow  — team[] → interchanges[] (with on/off child elements)
                     team: isHomeTeam, teamId, teamName
                     interchanges: interchangeNumber, interchangeCount, interchangeUsed,
                       interchangeLeft, gameStateId, gameState, gameTime, gameSeconds,
                       gameMinutes, sca
                     on/off: playerId, shirtNum, playerName, statID
                     NOTE: on element may be absent (injury-off only)

  commentaryFlow   — event[]
                     attrs: teamId, teamName, gameState, gameStateId, playerId (opt),
                       isHomeTeam, statId, gameTime, gameSeconds, gameMinutes, period,
                       commentary, homeScore, awayScore, tackleNum, utcTime (opt)

  colourEventsFlow — event[]
                     attrs: teamId, teamName, isHomeTeam, statId, gameTime,
                       gameSeconds, gameMinutes, commentary, homeScore, awayScore,
                       tackleNum, utcTime, playerId (opt)

  disciplineFlow   — team[] → events (sin-bin / send-off per team)
                     team: isHomeTeam, teamId, teamName
                     events attrs: disciplineNumber, gameStateId, gameState,
                       gameTime, gameSeconds, gameMinutes, yellow, red,
                       playerId, playerName, utcTime
                     NOTE: teams with no discipline events carry no <events> child

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.nrl_event_flow):
  nrl_event_flow              ← root (one row per game)
  nrl_event_flow_score_team   ← team scoring summary per game
  nrl_event_flow_score_event  ← individual scoring events
  nrl_event_flow_possession   ← possession / set events
  nrl_event_flow_penalty      ← penalty events
  nrl_event_flow_error        ← error / turnover events
  nrl_event_flow_scrum        ← scrum events
  nrl_event_flow_interchange  ← player interchange events
  nrl_event_flow_commentary   ← play-by-play commentary events
  nrl_event_flow_colour       ← colour / broadcast events
  nrl_event_flow_discipline   ← discipline / sin-bin / send-off events

NOTE: all eleven tables are created by post_run_fn.
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
def _create_event_flow_tables(config, spark):
    """
    Creates eleven doc-aligned tables from the eventflow bronze data.

    nrl_event_flow             — one row per game (l0payload root attrs)
    nrl_event_flow_score_team  — one row per team per game (scoring summary)
    nrl_event_flow_score_event — one row per scoring event
    nrl_event_flow_possession  — one row per possession/set event
    nrl_event_flow_penalty     — one row per penalty event
    nrl_event_flow_error       — one row per error/turnover event
    nrl_event_flow_scrum       — one row per scrum event
    nrl_event_flow_interchange — one row per interchange event
    nrl_event_flow_commentary  — one row per commentary event
    nrl_event_flow_colour      — one row per colour/broadcast event
    nrl_event_flow_discipline  — one row per discipline event
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
    # Root uses a dict keyed by (comp, season, game) so l0payload fields are
    # merged regardless of which l1element row is processed first.
    root_data           = {}   # (competition_id, season_id, game_id) → dict
    score_team_records  = []
    score_event_records = []
    possession_records  = []
    penalty_records     = []
    error_records       = []
    scrum_records       = []
    interchange_records = []
    commentary_records  = []
    colour_records      = []
    discipline_records  = []

    for row in rows:
        ingested_from = row.ingested_from
        ingested_at   = str(row.ingested_at)

        # Parse context: match_events_flow_{competitionId}_{seasonId}_..._{gameId}
        parts          = ingested_from.split("_")
        competition_id = parts[3] if len(parts) > 3 else None
        season_id      = parts[4] if len(parts) > 4 else None
        game_id        = parts[-1] if len(parts) > 4 else None

        ctx = {
            "ingested_from":     ingested_from,
            "ingested_at":       ingested_at,
            "competition_id":    competition_id,
            "season_id":         season_id,
            "game_id":           game_id,
            "source_schema":     "bronze_stats",
            "source_table_name": "eventflow",
            "target_schema":     "bronze_stats_flatten",
        }

        root_key = (competition_id, season_id, game_id)

        # ── Seed root record from l0payload (eventFlow root attrs) ─────────
        if row.l0:
            l0 = _json.loads(row.l0)
            if isinstance(l0, dict) and l0:
                if root_key not in root_data:
                    root_data[root_key] = {**ctx, "target_table_name": "nrl_event_flow"}
                root_data[root_key].update(_scalars(l0))

        payload = _json.loads(row.p) if row.p else {}

        # ── scoreFlow ─────────────────────────────────────────────────────────
        # Payload: {"scoreEvent": {"teams": {"team": [{...statScoringEvent[]}, ...]}}}
        if row.l1element == "scoreFlow":
            # Unwrap scoreEvent → teams → team[]
            inner = payload if isinstance(payload, dict) else {}
            inner = inner.get("scoreEvent") or inner
            if isinstance(inner, dict):
                teams_wrap = inner.get("teams") or inner
                if isinstance(teams_wrap, dict):
                    teams = _to_list(teams_wrap.get("team") or teams_wrap)
                else:
                    teams = _to_list(teams_wrap)
            else:
                teams = _to_list(inner)

            for team in teams:
                if not isinstance(team, dict):
                    continue

                team_id   = team.get("teamId")
                team_name = team.get("teamName")
                is_home   = team.get("isHomeTeam")

                # Team scoring summary row (all scalar attrs)
                score_team_records.append({**_scalars(team), **ctx})

                # Individual scoring events
                for event in _to_list(team.get("statScoringEvent")):
                    if not isinstance(event, dict):
                        continue
                    event_row = _scalars(event)
                    event_row["team_id"]      = team_id
                    event_row["team_name"]    = team_name
                    event_row["is_home_team"] = is_home
                    score_event_records.append({**event_row, **ctx})

        # ── possessionFlow ────────────────────────────────────────────────────
        # Payload: {"possessionEvent": {"statPossessionEvent": [...]}}
        elif row.l1element == "possessionFlow":
            inner = payload if isinstance(payload, dict) else {}
            inner = inner.get("possessionEvent") or inner
            if isinstance(inner, dict):
                events = _to_list(inner.get("statPossessionEvent") or inner)
            else:
                events = _to_list(inner)

            for event in events:
                if not isinstance(event, dict):
                    continue
                possession_records.append({**_scalars(event), **ctx})

        # ── penaltyFlow ───────────────────────────────────────────────────────
        # Payload: {"penaltyEvent": {"statPenaltyEvent": [...]}}
        elif row.l1element == "penaltyFlow":
            inner = payload if isinstance(payload, dict) else {}
            inner = inner.get("penaltyEvent") or inner
            if isinstance(inner, dict):
                events = _to_list(inner.get("statPenaltyEvent") or inner)
            else:
                events = _to_list(inner)

            for event in events:
                if not isinstance(event, dict):
                    continue
                penalty_records.append({**_scalars(event), **ctx})

        # ── errorFlow ─────────────────────────────────────────────────────────
        # Payload: {"errorEvent": {"statErrorEvent": [...]}}
        elif row.l1element == "errorFlow":
            inner = payload if isinstance(payload, dict) else {}
            inner = inner.get("errorEvent") or inner
            if isinstance(inner, dict):
                events = _to_list(inner.get("statErrorEvent") or inner)
            else:
                events = _to_list(inner)

            for event in events:
                if not isinstance(event, dict):
                    continue
                error_records.append({**_scalars(event), **ctx})

        # ── scrumFlow ─────────────────────────────────────────────────────────
        # Payload: {"scrumEvent": {"statScrumEvent": [...]}}
        elif row.l1element == "scrumFlow":
            inner = payload if isinstance(payload, dict) else {}
            inner = inner.get("scrumEvent") or inner
            if isinstance(inner, dict):
                events = _to_list(inner.get("statScrumEvent") or inner)
            else:
                events = _to_list(inner)

            for event in events:
                if not isinstance(event, dict):
                    continue
                scrum_records.append({**_scalars(event), **ctx})

        # ── interchangeFlow ───────────────────────────────────────────────────
        # Payload: {"team": [{teamId, teamName, isHomeTeam,
        #            interchanges: [{..., on: {...}, off: {...}}]}]}
        # NOTE: on element may be absent for injury-off records.
        elif row.l1element == "interchangeFlow":
            if isinstance(payload, dict):
                teams = _to_list(payload.get("team") or payload)
            else:
                teams = _to_list(payload)

            for team in teams:
                if not isinstance(team, dict):
                    continue

                team_id   = team.get("teamId")
                team_name = team.get("teamName")
                is_home   = team.get("isHomeTeam")

                for interchange in _to_list(team.get("interchanges")):
                    if not isinstance(interchange, dict):
                        continue

                    event_row = _scalars(interchange)

                    # Flatten on/off child elements with direction prefix
                    for direction in ("on", "off"):
                        node = interchange.get(direction)
                        if isinstance(node, dict):
                            for k, v in node.items():
                                event_row[f"{direction}_{_to_snake(k)}"] = v

                    event_row["team_id"]      = team_id
                    event_row["team_name"]    = team_name
                    event_row["is_home_team"] = is_home

                    interchange_records.append({**event_row, **ctx})

        # ── commentaryFlow ────────────────────────────────────────────────────
        # Payload: {"event": [...]}
        elif row.l1element == "commentaryFlow":
            if isinstance(payload, dict):
                events = _to_list(payload.get("event") or payload)
            else:
                events = _to_list(payload)

            for event in events:
                if not isinstance(event, dict):
                    continue
                commentary_records.append({**_scalars(event), **ctx})

        # ── colourEventsFlow ──────────────────────────────────────────────────
        # Payload: {"event": [...]}
        elif row.l1element == "colourEventsFlow":
            if isinstance(payload, dict):
                events = _to_list(payload.get("event") or payload)
            else:
                events = _to_list(payload)

            for event in events:
                if not isinstance(event, dict):
                    continue
                colour_records.append({**_scalars(event), **ctx})

        # ── disciplineFlow ────────────────────────────────────────────────────
        # Payload: {"team": [{teamId, teamName, isHomeTeam, events: {...}}]}
        # Teams with no discipline events carry no <events> child — skipped safely.
        elif row.l1element == "disciplineFlow":
            if isinstance(payload, dict):
                teams = _to_list(payload.get("team") or payload)
            else:
                teams = _to_list(payload)

            for team in teams:
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
            print(f"[eventflow] No records for {table_name} — skipping")
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
        print(f"[eventflow] {len(_rows)} rows -> {table_name} (mode={write_mode})")

    _write(root_records,         f"{target_prefix}")
    _write(score_team_records,   f"{target_prefix}_score_team")
    _write(score_event_records,  f"{target_prefix}_score_event")
    _write(possession_records,   f"{target_prefix}_possession")
    _write(penalty_records,      f"{target_prefix}_penalty")
    _write(error_records,        f"{target_prefix}_error")
    _write(scrum_records,        f"{target_prefix}_scrum")
    _write(interchange_records,  f"{target_prefix}_interchange")
    _write(commentary_records,   f"{target_prefix}_commentary")
    _write(colour_records,       f"{target_prefix}_colour")
    _write(discipline_records,   f"{target_prefix}_discipline")


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
EVENTFLOW_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.match_events_flow",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_event_flow",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'match_events_flow_{competition_id}_{season}%'",

    # All nested structures are owned by post_run_fn — exclude from
    # framework auto-discovery to avoid partial / duplicate tables.
    "exclude_fields": {
        "scoreEvent",
        "teams",
        "team",
        "statScoringEvent",
        "possessionEvent",
        "statPossessionEvent",
        "penaltyEvent",
        "statPenaltyEvent",
        "errorEvent",
        "statErrorEvent",
        "scrumEvent",
        "statScrumEvent",
        "interchanges",
        "on",
        "off",
        "event",
        "events",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "scoreFlow":        "score_flow",
        "possessionFlow":   "possession_flow",
        "penaltyFlow":      "penalty_flow",
        "errorFlow":        "error_flow",
        "scrumFlow":        "scrum_flow",
        "interchangeFlow":  "interchange_flow",
        "commentaryFlow":   "commentary_flow",
        "colourEventsFlow": "colour_events_flow",
        "disciplineFlow":   "discipline_flow",
    },

    "dedup_keys_map": {
        "score_flow":         ["ingested_from"],
        "possession_flow":    ["ingested_from"],
        "penalty_flow":       ["ingested_from"],
        "error_flow":         ["ingested_from"],
        "scrum_flow":         ["ingested_from"],
        "interchange_flow":   ["ingested_from"],
        "commentary_flow":    ["ingested_from"],
        "colour_events_flow": ["ingested_from"],
        "discipline_flow":    ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Suppress all framework-generated root tables; all tables owned by post_run_fn.
    "exclude_root_aliases": {
        "score_flow",
        "possession_flow",
        "penalty_flow",
        "error_flow",
        "scrum_flow",
        "interchange_flow",
        "commentary_flow",
        "colour_events_flow",
        "discipline_flow",
    },

    # Creates: nrl_event_flow,
    #          nrl_event_flow_score_team,
    #          nrl_event_flow_score_event,
    #          nrl_event_flow_possession,
    #          nrl_event_flow_penalty,
    #          nrl_event_flow_error,
    #          nrl_event_flow_scrum,
    #          nrl_event_flow_interchange,
    #          nrl_event_flow_commentary,
    #          nrl_event_flow_colour,
    #          nrl_event_flow_discipline
    "post_run_fn": _create_event_flow_tables,
}
