"""
NRL Form Guide For Match API — Flattening Config
Source  : api/NRL/formGuide/{competitionId}/{seasonId}/{gameId}
Bronze  : nrl_datalakehouse_prd.bronze_stats.formguideformatch
Mode    : l0payload is an OBJECT (game context); payload per l1element is an array.
Elements: l1element = 'lastTenMeetings' | 'teamFormGuideStats'

ingested_from pattern : formGuideForMatch_{competitionId}_{seasonId}_{gameId}
e.g. formGuideForMatch_111_2026_20261110110

l0payload (object — root <formGuide> attributes):
  { competitionId, competitionName, gameHashTag, gameId, roundId, seasonId,
    startTime, startTimeUTC, venueId, venueName }

l1element = 'lastTenMeetings'  — array of <game> elements:
  { isHomeTeam, awayTeamScore, gameId, homeTeamScore,
    matchSummary, roundId, roundName, seasonId }

l1element = 'teamFormGuideStats'  — ONE object per team (2 rows per game):
  Attributes : isHomeTeam, teamAbbr, teamId, teamName, teamPosition,
               premiershipOdds, premiershipOddsStatus, premiershipOddsOutcomeID
  Children:
    headToHead              — scalar attrs (teamGamesPlayedVsOpposingTeam, odds, ...)
    recentRecord
      └── recentGame[]      — scalar attrs per recent game
    seasonHomeAndAwayAverages
      ├── home              — scalar stats (allRunMetres, tries, possession, ...)
      └── away              — same fields
    seasonPlayerStats
      └── playerGame[]      — appearances, playerId, playerName
    seasonTeamStats         — scalar attrs (allRunMetresPerGame, wins, losses, ...)
    seasonWinLossBreakdown  — scalar attrs (winWin, lossLoss, half1Win, ...)
    teamList
      └── playerLineup[]    — playerFirstName, playerId, playerLastName,
                              playerName, shirtNum

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.nrl_form_guide):
  nrl_form_guide                    ← root (one row per game)
  nrl_form_guide_last_ten_meeting   ← last 10 H2H meetings
  nrl_form_guide_team               ← one row per team (attributes)
  nrl_form_guide_h2h                ← head-to-head stats per team
  nrl_form_guide_recent_game        ← recent game results per team
  nrl_form_guide_season_home_away   ← home & away season averages per team
  nrl_form_guide_player_game        ← season player appearances per team
  nrl_form_guide_season_team_stats  ← season team stats per team
  nrl_form_guide_win_loss           ← win/loss breakdown per team
  nrl_form_guide_team_lineup        ← named squad with shirt numbers per team

NOTE: l0payload carries the game context; all ten tables are created by post_run_fn.
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
def _create_form_guide_for_match_tables(config, spark):
    """
    Creates ten doc-aligned tables from the formguideformatch bronze data.

    nrl_form_guide                   — one row per game (from l0payload)
    nrl_form_guide_last_ten_meeting  — one row per H2H meeting
    nrl_form_guide_team              — one row per team per game
    nrl_form_guide_h2h               — H2H stats per team
    nrl_form_guide_recent_game       — recent game per team
    nrl_form_guide_season_home_away  — home & away averages per team × side
    nrl_form_guide_player_game       — season player appearances per team
    nrl_form_guide_season_team_stats — season stats per team
    nrl_form_guide_win_loss          — win/loss breakdown per team
    nrl_form_guide_team_lineup       — squad with shirt numbers per team
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
    root_records              = []
    last_ten_records          = []
    team_records              = []
    h2h_records               = []
    recent_game_records       = []
    season_ha_records         = []
    player_game_records       = []
    player_game_stats_records = []
    season_stats_records      = []
    win_loss_records          = []
    team_lineup_records       = []
    seen_root                 = set()

    for row in rows:
        ingested_from = row.ingested_from
        ingested_at   = str(row.ingested_at)

        # Parse context: form_guide_for_match_{competitionId}_{seasonId}_..._{gameId}
        parts          = ingested_from.split("_")
        competition_id = parts[4] if len(parts) > 4 else None
        season_id      = parts[5] if len(parts) > 5 else None
        game_id        = parts[-1] if len(parts) > 5 else None

        ctx = {
            "ingested_from":     ingested_from,
            "ingested_at":       ingested_at,
            "competition_id":    competition_id,
            "season_id":         season_id,
            "game_id":           game_id,
            "source_schema":     "bronze_stats",
            "source_table_name": "form_guide_for_match",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Root row — built once per game from l0payload ─────────────────────
        root_key = (competition_id, season_id, game_id)
        if root_key not in seen_root and row.l0:
            l0 = _json.loads(row.l0)
            if isinstance(l0, dict) and l0:
                seen_root.add(root_key)
                root_row = _scalars(l0)
                root_row["target_table_name"] = "nrl_form_guide"
                root_records.append({**root_row, **ctx})

        payload = _json.loads(row.p) if row.p else []

        # ── lastTenMeetings ───────────────────────────────────────────────────
        # Payload: array of <game> objects, or {"game": [...]} when XML-converted
        if row.l1element == "lastTenMeetings":
            if isinstance(payload, dict):
                meetings = _to_list(payload.get("game") or payload)
            else:
                meetings = _to_list(payload)

            for meeting in meetings:
                if not isinstance(meeting, dict):
                    continue
                last_ten_records.append({**_scalars(meeting), **ctx})

        # ── teamFormGuideStats ────────────────────────────────────────────────
        # Payload: one team object per bronze row (or array of 2 team objects)
        elif row.l1element == "teamFormGuideStats":
            if isinstance(payload, dict):
                # Single team object or {"teamFormGuideStats": [...]}
                teams = _to_list(payload.get("teamFormGuideStats") or [payload])
            else:
                teams = _to_list(payload)

            for team in teams:
                if not isinstance(team, dict):
                    continue

                team_id   = team.get("teamId")
                team_name = team.get("teamName")
                is_home   = team.get("isHomeTeam")

                # FK carried on every child row so they join back to _team
                team_fk = {
                    "team_id":      team_id,
                    "team_name":    team_name,
                    "is_home_team": is_home,
                }

                # ── team row (teamFormGuideStats scalar attributes) ───────────
                team_records.append({**_scalars(team), **ctx})

                # ── headToHead ───────────────────────────────────────────────
                h2h = team.get("headToHead") or {}
                if isinstance(h2h, dict) and h2h:
                    h2h_records.append({**_scalars(h2h), **team_fk, **ctx})

                # ── recentRecord → recentGame[] ──────────────────────────────
                recent_record = team.get("recentRecord") or {}
                if isinstance(recent_record, dict):
                    for game in _to_list(recent_record.get("recentGame")):
                        if not isinstance(game, dict):
                            continue
                        recent_game_records.append({**_scalars(game), **team_fk, **ctx})

                # ── seasonHomeAndAwayAverages → home / away ──────────────────
                season_ha = team.get("seasonHomeAndAwayAverages") or {}
                if isinstance(season_ha, dict):
                    for side in ("home", "away"):
                        side_data = season_ha.get(side)
                        if isinstance(side_data, dict):
                            side_row = _scalars(side_data)
                            side_row["team_side"] = side
                            season_ha_records.append({**side_row, **team_fk, **ctx})

                # ── seasonPlayerStats → playerGame[] ─────────────────────────
                season_players = team.get("seasonPlayerStats") or {}
                if isinstance(season_players, dict):
                    for pg in _to_list(season_players.get("playerGame")):
                        if not isinstance(pg, dict):
                            continue

                        player_id   = pg.get("playerId")
                        player_name = pg.get("playerName")
                        player_fk   = {
                            "player_id":   player_id,
                            "player_name": player_name,
                        }

                        # team_player_stats: scalar attributes only
                        player_game_records.append({**_scalars(pg), **team_fk, **ctx})

                        # player_game_stats: averageStats, maxStats, totalStats inlined
                        stats_row = {}
                        for avg_k, avg_v in (pg.get("averageStats") or {}).items():
                            stats_row[_to_snake(avg_k)] = avg_v
                        for max_k, max_v in (pg.get("maxStats") or {}).items():
                            stats_row[_to_snake(max_k)] = max_v
                        for tot_k, tot_v in (pg.get("totalStats") or {}).items():
                            stats_row[f"total_{_to_snake(tot_k)}"] = tot_v
                        if stats_row:
                            player_game_stats_records.append({**stats_row, **player_fk, **team_fk, **ctx})

                # ── seasonTeamStats ──────────────────────────────────────────
                season_stats = team.get("seasonTeamStats") or {}
                if isinstance(season_stats, dict) and season_stats:
                    season_stats_records.append({**_scalars(season_stats), **team_fk, **ctx})

                # ── seasonWinLossBreakdown ───────────────────────────────────
                win_loss = team.get("seasonWinLossBreakdown") or {}
                if isinstance(win_loss, dict) and win_loss:
                    win_loss_records.append({**_scalars(win_loss), **team_fk, **ctx})

                # ── teamList → playerLineup[] ────────────────────────────────
                team_list = team.get("teamList") or {}
                if isinstance(team_list, dict):
                    for lineup in _to_list(team_list.get("playerLineup")):
                        if not isinstance(lineup, dict):
                            continue
                        team_lineup_records.append({**_scalars(lineup), **team_fk, **ctx})

    # ── Write tables ──────────────────────────────────────────────────────────
    write_mode = config.get("write_mode", "overwrite")

    def _write(records, table_name):
        if not records:
            print(f"[formguideformatch] No records for {table_name} — skipping")
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
        print(f"[formguideformatch] {len(_rows)} rows -> {table_name} (mode={write_mode})")

    _write(root_records,         f"{target_prefix}")
    _write(last_ten_records,     f"{target_prefix}_last_ten_meeting")
    _write(team_records,         f"{target_prefix}_team")
    _write(h2h_records,          f"{target_prefix}_h2h")
    _write(recent_game_records,  f"{target_prefix}_team_recent_game")
    _write(season_ha_records,    f"{target_prefix}_season_home_away_averages")
    _write(player_game_records,       f"{target_prefix}_team_player_stats")
    _write(player_game_stats_records, f"{target_prefix}_player_game_stats")
    _write(season_stats_records,      f"{target_prefix}_season_team_stats")
    _write(win_loss_records,     f"{target_prefix}_season_win_loss_breakdown")
    _write(team_lineup_records,  f"{target_prefix}_player_line_up")


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
FORMGUIDEFORMATCH_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.form_guide_for_match",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_form_guide",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'form_guide_for_match_{competition_id}_{season}%'",

    # All nested structures are owned by post_run_fn — exclude from
    # framework auto-discovery to avoid partial / duplicate tables.
    "exclude_fields": {
        "headToHead",
        "recentRecord",
        "recentGame",
        "seasonHomeAndAwayAverages",
        "home",
        "away",
        "seasonPlayerStats",
        "playerGame",
        "seasonTeamStats",
        "seasonWinLossBreakdown",
        "teamList",
        "playerLineup",
        "game",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "lastTenMeetings":    "last_ten_meetings",
        "teamFormGuideStats": "team_form_guide_stats",
    },

    "dedup_keys_map": {
        "last_ten_meetings":     ["ingested_from"],
        "team_form_guide_stats": ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Only propagate key/ID columns to child tables.
    # Descriptive l0payload fields (competition_name, game_hash_tag, venue_name,
    # start_time, etc.) stay in the root table but do not flow to child tables.
    "passthrough_fields": {
        "ingested_from",
        "ingested_at",
        "competition_id",
        "season_id",
        "game_id",
        "round_id",
    },

    # Suppress framework root table for lastTenMeetings only; team_form_guide_stats
    # is kept so premiershipOdds / teamPosition data is preserved in _team table.
    "exclude_root_aliases": {"last_ten_meetings"},

    # Creates: nrl_form_guide,
    #          nrl_form_guide_last_ten_meeting,
    #          nrl_form_guide_team,
    #          nrl_form_guide_h2h,
    #          nrl_form_guide_team_recent_game,
    #          nrl_form_guide_season_home_away_average,
    #          nrl_form_guide_team_player_stats,
    #          nrl_form_guide_season_team_stats,
    #          nrl_form_guide_season_win_loss_breakdown,
    #          nrl_form_guide_player_line_up
    "post_run_fn": _create_form_guide_for_match_tables,
}
