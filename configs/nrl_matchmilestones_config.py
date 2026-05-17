"""
NRL Match Milestones API — Flattening Config
Source  : api/NRL/matchMilestones/{competitionId}/{seasonId}/{gameId}
Bronze  : nrl_datalakehouse_prd.bronze_stats.upcoming_match_milestones
Mode    : l0payload is an OBJECT (game context); payload per l1element is an array.
Elements: l1element = 'teams'

ingested_from pattern : matchMilestones_{competitionId}_{seasonId}_{gameId}
e.g. matchMilestones_111_2026_20261110110

l0payload (object — game context attributes on <matchMilestones> root):
  { gameId, Team1ID, Team1Name, Team2ID, Team2Name,
    StartDateTimeUTC, StartDateTime, competitionId, competitionName }

l1element = 'teams' payload — array of team objects:
  team { TeamID, TeamName,
    teamStats {
      stats {
        leaderboardStat [ { ID, statName,
          milestoneEntry { Value, RequiredAmount, Achieved }   ← single dict, no player
        } ]
      }
    },
    playerStats {
      stats {
        leaderboardStat [ { ID, statName,
          milestoneEntry [ { PlayerID, PlayerName,             ← one or many players
                             Value, RequiredAmount, Achieved } ]
        } ]
      }
    }
  }

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.nrl_upcoming_match_milestones):
  nrl_upcoming_match_milestones                  ← root  (one row per game)
  nrl_upcoming_match_milestones_team             ← one row per team × leaderboardStat (team milestones)
  nrl_upcoming_match_milestones_player_stat      ← one row per leaderboardStat × team (stat header)
  nrl_upcoming_match_milestones_player_stat_entry← one row per player × leaderboardStat × team

NOTE: l0payload carries the game context; all three tables are created by post_run_fn.
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
def _create_match_milestones_tables(config, spark):
    """
    Creates four doc-aligned tables from the upcoming_match_milestones bronze data.

    nrl_upcoming_match_milestones                   — one row per game (from l0payload)
    nrl_upcoming_match_milestones_team              — one row per team × leaderboardStat (team milestones)
    nrl_upcoming_match_milestones_player_stat       — one row per leaderboardStat × team (stat header)
    nrl_upcoming_match_milestones_player_stat_entry — one row per player × leaderboardStat × team
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
    team_stat_records         = []
    player_stat_records       = []
    player_stat_entry_records = []
    seen_root                 = set()
    seen_player_stat          = set()

    for row in rows:
        ingested_from = row.ingested_from
        ingested_at   = str(row.ingested_at)

        # Parse context: upcoming_match_milestones_{competitionId}_{seasonId}_..._{gameId}
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
            "source_table_name": "upcoming_match_milestones",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Root row — built once per game from l0payload ─────────────────────
        root_key = (competition_id, season_id, game_id)
        if root_key not in seen_root and row.l0:
            l0 = _json.loads(row.l0)
            if isinstance(l0, dict) and l0:
                seen_root.add(root_key)
                root_row = _scalars(l0)
                root_row["target_table_name"] = "nrl_upcoming_match_milestones"
                root_records.append({**root_row, **ctx})

        payload = _json.loads(row.p) if row.p else []

        # Normalise: payload may arrive as the teams array directly,
        # or wrapped as {"team": [...]} when converted from XML
        if isinstance(payload, dict):
            payload = _to_list(payload.get("team") or payload)
        else:
            payload = _to_list(payload)

        # ── teams ─────────────────────────────────────────────────────────────
        if row.l1element == "teams":
            for team in payload:
                if not isinstance(team, dict):
                    continue

                team_id   = team.get("TeamID")
                team_name = team.get("TeamName")

                # ── teamStats: one milestoneEntry per leaderboardStat (no player) ─
                team_stats_node = (team.get("teamStats") or {}).get("stats") or {}
                for lb_stat in _to_list(team_stats_node.get("leaderboardStat")):
                    if not isinstance(lb_stat, dict):
                        continue

                    stat_id   = lb_stat.get("ID")
                    stat_name = lb_stat.get("statName")

                    for entry in _to_list(lb_stat.get("milestoneEntry")):
                        if not isinstance(entry, dict):
                            continue
                        team_stat_records.append({
                            "team_id":         team_id,
                            "team_name":       team_name,
                            "stat_id":         stat_id,
                            "stat_name":       stat_name,
                            "value":           entry.get("Value"),
                            "required_amount": entry.get("RequiredAmount"),
                            "achieved":        entry.get("Achieved"),
                            **ctx,
                        })

                # ── playerStats: one or many milestoneEntries per leaderboardStat ─
                player_stats_node = (team.get("playerStats") or {}).get("stats") or {}
                for lb_stat in _to_list(player_stats_node.get("leaderboardStat")):
                    if not isinstance(lb_stat, dict):
                        continue

                    stat_id   = lb_stat.get("ID")
                    stat_name = lb_stat.get("statName")

                    # Stat-level row — one per (game, team, stat)
                    ps_key = (competition_id, season_id, game_id, team_id, stat_id)
                    if ps_key not in seen_player_stat:
                        seen_player_stat.add(ps_key)
                        player_stat_records.append({
                            "team_id":   team_id,
                            "team_name": team_name,
                            "stat_id":   stat_id,
                            "stat_name": stat_name,
                            **ctx,
                        })

                    # Entry-level rows — one per player
                    for entry in _to_list(lb_stat.get("milestoneEntry")):
                        if not isinstance(entry, dict):
                            continue
                        player_stat_entry_records.append({
                            "stat_id":         stat_id,
                            "stat_name":       stat_name,
                            "team_id":         team_id,
                            "team_name":       team_name,
                            "player_id":       entry.get("PlayerID"),
                            "player_name":     entry.get("PlayerName"),
                            "value":           entry.get("Value"),
                            "required_amount": entry.get("RequiredAmount"),
                            "achieved":        entry.get("Achieved"),
                            **ctx,
                        })

    # ── Write tables ──────────────────────────────────────────────────────────
    write_mode = config.get("write_mode", "overwrite")

    def _write(records, table_name):
        if not records:
            print(f"[upcoming_match_milestones] No records for {table_name} — skipping")
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
        print(f"[upcoming_match_milestones] {len(_rows)} rows -> {table_name} (mode={write_mode})")

    _write(root_records,              f"{target_prefix}")
    _write(team_stat_records,         f"{target_prefix}_team")
    _write(player_stat_records,       f"{target_prefix}_player_stat")
    _write(player_stat_entry_records, f"{target_prefix}_player_stat_entry")


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
MATCHMILESTONES_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.upcoming_match_milestones",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_upcoming_match_milestones",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'upcoming_match_milestones_{competition_id}_{season}%'",

    # All nested structures are handled by post_run_fn — exclude from
    # framework auto-discovery to avoid partial / duplicate tables.
    "exclude_fields": {
        "teamStats",
        "playerStats",
        "stats",
        "leaderboardStat",
        "milestoneEntry",
        "team",
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

    # Creates: nrl_upcoming_match_milestones,
    #          nrl_upcoming_match_milestones_team,
    #          nrl_upcoming_match_milestones_player_stat,
    #          nrl_upcoming_match_milestones_player_stat_entry
    "post_run_fn": _create_match_milestones_tables,
}
