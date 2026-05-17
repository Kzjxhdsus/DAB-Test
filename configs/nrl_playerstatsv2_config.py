"""
NRL Player Stats V2 API — Flattening Config
Source  : api/NRL/playerStatsV2/{competitionId}/{seasonId}
Bronze  : nrl_datalakehouse_qa.bronze_stats.player_stats_v2
Mode    : l0payload is an OBJECT (<playerStatsV2> root attributes).
Elements: l1element = 'players'                       — array of player objects
          l1element = 'competitionPlayerAverageStats'  — competition-level average stats

ingested_from pattern : player_stats_v2_{competitionId}_{seasonId}
e.g. player_stats_v2_111_2026

l0payload (object — <playerStatsV2> root attributes):
  { competitionId, competitionName, seasonId }

l1element = 'players' payload — array of player objects:
  player { playerFirstName, playerId, playerLastName, playerName, playerNickName,
           playerBirthPlace, positionId, positionName, shirtNum,
           teamId, teamName, headshotUrl, bodyshotUrl,
           fantasyPrice, fantasySeasonPriceChange, fantasyRoundPriceChange,
    career {
      careerStatsTotal  { [flat attrs] },
      careerStatsAverage { [flat attrs] },
      teams { team [ { teamId, teamName, isCurrentTeam,
        teamStatsTotal   { [flat attrs] },
        teamStatsAverage { [flat attrs] }
      } ] }
    },
    season [ { seasonId,
      seasonStatsTotal { [flat attrs],
        roundStats { round [ { roundNumber, gameID, oppositionId, teamId, [flat attrs] } ] }
      },
      seasonStatsAverage { [flat attrs] },
      teams { team [ { teamId, teamName, isCurrentTeam,
        teamStatsTotal   { [flat attrs] },
        teamStatsAverage { [flat attrs] }
      } ] }
    } ]
  }

l1element = 'competitionPlayerAverageStats' payload — flat stat attrs object:
  { [flat stat attrs] }

Output tables (target_prefix = nrl_datalakehouse_qa.bronze_stats_flatten.nrl_player_stats_v2):
  nrl_player_stats_v2                       ← Level 0   Root (one row per competition+season)
  nrl_player_stats_v2_competition_average   ← Level 0b  Competition Average
  nrl_player_stats_v2_player                ← Level 1   Player (one row per player per ingestion)
  nrl_player_stats_v2_career_total          ← Level 2a  Career Total
  nrl_player_stats_v2_career_average        ← Level 2b  Career Average
  nrl_player_stats_v2_career_team_total     ← Level 3a  Career × Team Total
  nrl_player_stats_v2_career_team_average   ← Level 3b  Career × Team Average
  nrl_player_stats_v2_season_total          ← Level 4a  Season Total (excl. roundStats)
  nrl_player_stats_v2_season_average        ← Level 4b  Season Average
  nrl_player_stats_v2_season_team_total     ← Level 4c  Season × Team Total
  nrl_player_stats_v2_season_team_average   ← Level 4d  Season × Team Average
  nrl_player_stats_v2_round                 ← Level 5   Round (one row per game per player)

NOTE: all tables are created by post_run_fn.
      career_team_id / season_team_id carry the FK to the per-team stat nodes;
      team_id / team_name on every row is the player's current team (from player attrs).
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
        print(f"[playerstatsv2] No records for {table_name} — skipping")
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
    print(f"[playerstatsv2] {len(rows)} rows → {table_name} (mode={write_mode})")


# ---------------------------------------------------------------------------
# post_run_fn
# ---------------------------------------------------------------------------
def _create_player_stats_v2_tables(config, spark):
    """
    Creates twelve doc-aligned tables from the playerStatsV2 bronze data.

    nrl_player_stats_v2                     — one row per competition+season (root context)
    nrl_player_stats_v2_competition_average — one row per competition+season (comp-level avg stats)
    nrl_player_stats_v2_player              — one row per player per ingestion
    nrl_player_stats_v2_career_total        — one row per player (career totals)
    nrl_player_stats_v2_career_average      — one row per player (career per-game averages)
    nrl_player_stats_v2_career_team_total   — one row per player per career team (totals)
    nrl_player_stats_v2_career_team_average — one row per player per career team (averages)
    nrl_player_stats_v2_season_total        — one row per player per season (season totals)
    nrl_player_stats_v2_season_average      — one row per player per season (season averages)
    nrl_player_stats_v2_season_team_total   — one row per player per season per team (totals)
    nrl_player_stats_v2_season_team_average — one row per player per season per team (averages)
    nrl_player_stats_v2_round               — one row per player per game (round-level stats)
    """
    bronze = config["bronze_table"]
    prefix = config["target_prefix"]
    bf     = config.get("bronze_filter")

    bronze_df = spark.table(bronze)
    if bf:
        bronze_df = bronze_df.filter(bf)

    rows = (
        bronze_df
        .filter(F.col("l1element").isin("players", "competitionPlayerAverageStats"))
        .select(
            "ingested_from",
            "ingested_at",
            "l1element",
            F.col("payload").cast("string").alias("p"),
            F.col("l0payload").cast("string").alias("l0"),
        )
        .collect()
    )

    root_records            = []
    comp_avg_records        = []
    player_records          = []
    career_total_recs       = []
    career_avg_recs         = []
    career_team_total_recs  = []
    career_team_avg_recs    = []
    season_total_recs       = []
    season_avg_recs         = []
    season_team_total_recs  = []
    season_team_avg_recs    = []
    round_records           = []

    seen_root     = set()
    seen_comp_avg = set()

    for row in rows:
        # Parse context: player_stats_v2_{competitionId}_{seasonId}
        parts          = row.ingested_from.split("_")
        competition_id = parts[3] if len(parts) > 3 else None
        season_id      = parts[4] if len(parts) > 4 else None

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
            "source_schema":     "bronze_stats",
            "source_table_name": "player_stats_v2",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Level 0: Root row — one per competition+season ────────────────────
        root_key = (competition_id, season_id)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name":  competition_name,
                "target_table_name": "nrl_player_stats_v2",
            })

        # ── Level 0b: Competition average — own l1element row ────────────────
        if row.l1element == "competitionPlayerAverageStats":
            comp_avg_key = (competition_id, season_id)
            if comp_avg_key not in seen_comp_avg:
                seen_comp_avg.add(comp_avg_key)
                payload = _json.loads(row.p) if row.p else {}
                if isinstance(payload, dict):
                    comp_avg_records.append({
                        **_scalars(payload),
                        **ctx,
                        "target_table_name": "nrl_player_stats_v2_competition_average",
                    })
            continue   # nothing else to process for this l1element

        # ── Players (l1element == 'players') ─────────────────────────────────
        payload = _json.loads(row.p) if row.p else []
        if isinstance(payload, dict):
            players = _to_list(payload.get("player") or payload)
        else:
            players = _to_list(payload)

        for player in players:
            if not isinstance(player, dict):
                continue

            # FK-only base context — only player_id flows to all child tables
            player_id   = player.get("playerId")
            player_base = {
                **ctx,
                "player_id": player_id,
            }

            # ── Level 1: Player row — full attrs here only ────────────────────
            player_records.append({
                **_scalars(player),  # all player attrs (name, position, URLs, etc.)
                **ctx,
                "target_table_name": "nrl_player_stats_v2_player",
            })

            # ── Career ────────────────────────────────────────────────────────
            career = player.get("career") or {}
            if isinstance(career, dict):

                # Level 2a: Career total
                career_total = career.get("careerStatsTotal") or {}
                if isinstance(career_total, dict) and career_total:
                    career_total_recs.append({
                        **_scalars(career_total),
                        **player_base,
                        "target_table_name": "nrl_player_stats_v2_career_total",
                    })

                # Level 2b: Career average
                career_avg = career.get("careerStatsAverage") or {}
                if isinstance(career_avg, dict) and career_avg:
                    career_avg_recs.append({
                        **_scalars(career_avg),
                        **player_base,
                        "target_table_name": "nrl_player_stats_v2_career_average",
                    })

                # Level 3a/3b: Career × team stats
                career_teams_node = career.get("teams") or {}
                career_team_list  = (
                    _to_list(career_teams_node.get("team"))
                    if isinstance(career_teams_node, dict)
                    else _to_list(career_teams_node)
                )
                for cteam in career_team_list:
                    if not isinstance(cteam, dict):
                        continue

                    cteam_base = {
                        **player_base,
                        "career_team_id":   cteam.get("teamId"),
                        "career_team_name": cteam.get("teamName"),
                        "is_current_team":  cteam.get("isCurrentTeam"),
                    }

                    ct_total = cteam.get("teamStatsTotal") or {}
                    if isinstance(ct_total, dict) and ct_total:
                        career_team_total_recs.append({
                            **_scalars(ct_total),
                            **cteam_base,
                            "target_table_name": "nrl_player_stats_v2_career_team_total",
                        })

                    ct_avg = cteam.get("teamStatsAverage") or {}
                    if isinstance(ct_avg, dict) and ct_avg:
                        career_team_avg_recs.append({
                            **_scalars(ct_avg),
                            **cteam_base,
                            "target_table_name": "nrl_player_stats_v2_career_team_average",
                        })

            # ── Season(s) — may be one dict or a list if multiple seasons ─────
            for season in _to_list(player.get("season")):
                if not isinstance(season, dict):
                    continue

                # Use the season's own seasonId; fall back to ingested_from season
                season_base = {
                    **player_base,
                    "season_id": season.get("seasonId") or season_id,
                }

                # Level 4a: Season total (seasonStatsTotal scalars, roundStats excluded)
                season_total = season.get("seasonStatsTotal") or {}
                if isinstance(season_total, dict) and season_total:
                    season_total_recs.append({
                        **_scalars(season_total),  # excludes nested roundStats dict automatically
                        **season_base,
                        "target_table_name": "nrl_player_stats_v2_season_total",
                    })

                    # ── Level 5: Round rows ────────────────────────────────────
                    round_stats_node = season_total.get("roundStats") or {}
                    round_list       = (
                        _to_list(round_stats_node.get("round"))
                        if isinstance(round_stats_node, dict)
                        else _to_list(round_stats_node)
                    )
                    for rd in round_list:
                        if not isinstance(rd, dict):
                            continue
                        round_records.append({
                            **_scalars(rd),
                            **season_base,
                            "target_table_name": "nrl_player_stats_v2_round",
                        })

                # Level 4b: Season average
                season_avg = season.get("seasonStatsAverage") or {}
                if isinstance(season_avg, dict) and season_avg:
                    season_avg_recs.append({
                        **_scalars(season_avg),
                        **season_base,
                        "target_table_name": "nrl_player_stats_v2_season_average",
                    })

                # Level 4c/4d: Season × team stats
                season_teams_node = season.get("teams") or {}
                season_team_list  = (
                    _to_list(season_teams_node.get("team"))
                    if isinstance(season_teams_node, dict)
                    else _to_list(season_teams_node)
                )
                for steam in season_team_list:
                    if not isinstance(steam, dict):
                        continue

                    steam_base = {
                        **season_base,
                        "season_team_id":   steam.get("teamId"),
                        "season_team_name": steam.get("teamName"),
                        "is_current_team":  steam.get("isCurrentTeam"),
                    }

                    st_total = steam.get("teamStatsTotal") or {}
                    if isinstance(st_total, dict) and st_total:
                        season_team_total_recs.append({
                            **_scalars(st_total),
                            **steam_base,
                            "target_table_name": "nrl_player_stats_v2_season_team_total",
                        })

                    st_avg = steam.get("teamStatsAverage") or {}
                    if isinstance(st_avg, dict) and st_avg:
                        season_team_avg_recs.append({
                            **_scalars(st_avg),
                            **steam_base,
                            "target_table_name": "nrl_player_stats_v2_season_team_average",
                        })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,            f"{prefix}",                        spark, write_mode)
    _write_records(comp_avg_records,        f"{prefix}_competition_average",    spark, write_mode)
    _write_records(player_records,          f"{prefix}_player",                 spark, write_mode)
    _write_records(career_total_recs,       f"{prefix}_career_total",           spark, write_mode)
    _write_records(career_avg_recs,         f"{prefix}_career_average",         spark, write_mode)
    _write_records(career_team_total_recs,  f"{prefix}_career_team_total",      spark, write_mode)
    _write_records(career_team_avg_recs,    f"{prefix}_career_team_average",    spark, write_mode)
    _write_records(season_total_recs,       f"{prefix}_season_total",           spark, write_mode)
    _write_records(season_avg_recs,         f"{prefix}_season_average",         spark, write_mode)
    _write_records(season_team_total_recs,  f"{prefix}_season_team_total",      spark, write_mode)
    _write_records(season_team_avg_recs,    f"{prefix}_season_team_average",    spark, write_mode)
    _write_records(round_records,           f"{prefix}_round",                  spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
PLAYERSTATSV2_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_qa.bronze_stats.player_stats_v2",
    "target_prefix": "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_player_stats_v2",

    # Always append so historical loads accumulate correctly.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'player_stats_v2_{competition_id}_{season}%'",

    # All nested structures handled by post_run_fn.
    "exclude_fields": {
        "player",
        "career",
        "careerStatsTotal",
        "careerStatsAverage",
        "teams",
        "team",
        "teamStatsTotal",
        "teamStatsAverage",
        "season",
        "seasonStatsTotal",
        "seasonStatsAverage",
        "roundStats",
        "round",
        "competitionPlayerAverageStats",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "players":                        "players",
        "competitionPlayerAverageStats":  "competitionPlayerAverageStats",
    },

    "dedup_keys_map": {
        "players":                       ["ingested_from"],
        "competitionPlayerAverageStats": ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Suppress framework-generated tables; all tables owned by post_run_fn.
    "exclude_root_aliases": {"players", "competitionPlayerAverageStats"},

    # Creates: nrl_player_stats_v2,
    #          nrl_player_stats_v2_competition_average,
    #          nrl_player_stats_v2_player,
    #          nrl_player_stats_v2_career_total,
    #          nrl_player_stats_v2_career_average,
    #          nrl_player_stats_v2_career_team_total,
    #          nrl_player_stats_v2_career_team_average,
    #          nrl_player_stats_v2_season_total,
    #          nrl_player_stats_v2_season_average,
    #          nrl_player_stats_v2_season_team_total,
    #          nrl_player_stats_v2_season_team_average,
    #          nrl_player_stats_v2_round
    "post_run_fn": _create_player_stats_v2_tables,
}
