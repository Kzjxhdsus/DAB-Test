"""
NRL Player Stats Master API — Flattening Config
Source  : api/NRL/playerStatsV2/{competitionId}/{seasonId}
Bronze  : nrl_datalakehouse_prd.bronze_stats.playerstatsmaster
Mode    : l0payload is an OBJECT (playerStatsV2 root attrs);
          payload for l1element 'players' is the full player array.
Elements: l1element = 'players'

ingested_from pattern : playerStatsMaster_{competitionId}_{seasonId}
e.g. playerStatsMaster_111_2026

l0payload (object — <playerStatsV2> root attributes):
  { competitionId, competitionName }

l1element = 'players' payload:
  {"player": [ <player>... ]}

  <player> attrs: playerFirstName, playerId, playerLastName, playerName,
    playerNickName, playerBirthPlace, positionId, positionName, shirtNum,
    teamId, teamName, headshotUrl, bodyshotUrl,
    fantasyPrice, fantasySeasonPriceChange, fantasyRoundPriceChange

  <career>
    <careerStatsTotal   ... />   ← scalar stat attrs (career aggregate totals)
    <careerStatsAverage ... />   ← scalar stat attrs (career per-game averages)
    <teams>
      <team teamName teamId isCurrentTeam>
        <teamStatsTotal   ... />
        <teamStatsAverage ... />
      </team>...
    </teams>

  <season seasonId="YYYY">      ← may be empty (<teams/>) for inactive players
    <seasonStatsTotal ...>       ← scalar stat attrs (season-to-date totals)
      <roundStats>
        <round ... />...         ← one row per round played (gameID, roundNumber, …)
      </roundStats>
    </seasonStatsTotal>
    <seasonStatsAverage ... />   ← scalar stat attrs (season-to-date averages)
    <teams>
      <team teamName teamId isCurrentTeam>
        <teamStatsTotal   ... />
        <teamStatsAverage ... />
      </team>...
    </teams>
  </season>

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.nrl_player_stats_master):
  nrl_player_stats_master                    ← root (one row per competition+season ingestion)
  nrl_player_stats_master_player             ← one row per player (profile attrs)
  nrl_player_stats_master_career_total       ← career aggregate totals per player
  nrl_player_stats_master_career_average     ← career per-game averages per player
  nrl_player_stats_master_career_team_total  ← career totals per player × team
  nrl_player_stats_master_career_team_avg    ← career averages per player × team
  nrl_player_stats_master_season_total       ← season-to-date totals per player
  nrl_player_stats_master_season_average     ← season-to-date averages per player
  nrl_player_stats_master_season_team_total  ← season totals per player × team
  nrl_player_stats_master_season_team_avg    ← season averages per player × team
  nrl_player_stats_master_round              ← per-round stats per player

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
def _create_player_stats_master_tables(config, spark):
    """
    Creates eleven doc-aligned tables from the playerstatsmaster bronze data.

    nrl_player_stats_master                   — one row per competition+season ingestion
    nrl_player_stats_master_player            — one row per player (profile attrs)
    nrl_player_stats_master_career_total      — career aggregate totals per player
    nrl_player_stats_master_career_average    — career per-game averages per player
    nrl_player_stats_master_career_team_total — career totals per player × team
    nrl_player_stats_master_career_team_avg   — career averages per player × team
    nrl_player_stats_master_season_total      — season-to-date totals per player
    nrl_player_stats_master_season_average    — season per-game averages per player
    nrl_player_stats_master_season_team_total — season totals per player × team
    nrl_player_stats_master_season_team_avg   — season averages per player × team
    nrl_player_stats_master_round             — one row per round per player
    """
    bronze        = config["bronze_table"]
    target_prefix = config["target_prefix"]

    _bronze_df = spark.table(bronze)
    _bf = config.get("bronze_filter")
    if _bf:
        _bronze_df = _bronze_df.filter(_bf)

    rows = (
        _bronze_df
        .filter(F.col("l1element") == "players")
        .select(
            F.col("l0payload").cast("string").alias("l0"),
            F.col("payload").cast("string").alias("p"),
            "ingested_from",
            "ingested_at",
        )
        .collect()
    )

    # ── accumulators ─────────────────────────────────────────────────────────
    root_records              = []
    player_records            = []
    career_total_records      = []
    career_avg_records        = []
    career_team_total_records = []
    career_team_avg_records   = []
    season_total_records      = []
    season_avg_records        = []
    season_team_total_records = []
    season_team_avg_records   = []
    round_records             = []
    seen_root                 = set()

    for row in rows:
        ingested_from = row.ingested_from
        ingested_at   = str(row.ingested_at)

        # Parse context: historical_stats_by_round_{competitionId}_{seasonId}
        parts          = ingested_from.split("_")
        competition_id = parts[4] if len(parts) > 4 else None
        season_id      = parts[5] if len(parts) > 5 else None

        ctx = {
            "ingested_from":     ingested_from,
            "ingested_at":       ingested_at,
            "competition_id":    competition_id,
            "season_id":         season_id,
            "source_schema":     "bronze_stats",
            "source_table_name": "historical_stats_by_round",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Root row — one per competition + season ───────────────────────────
        root_key = (competition_id, season_id)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_row = {"target_table_name": "nrl_player_stats_master"}
            if row.l0:
                l0 = _json.loads(row.l0)
                if isinstance(l0, dict):
                    root_row.update(_scalars(l0))
            root_records.append({**root_row, **ctx})

        payload = _json.loads(row.p) if row.p else {}

        # Unwrap {"player": [...]} or bare list
        if isinstance(payload, dict):
            players = _to_list(payload.get("player") or payload)
        else:
            players = _to_list(payload)

        # ── Iterate players ───────────────────────────────────────────────────
        for player in players:
            if not isinstance(player, dict):
                continue

            player_id = player.get("playerId")
            player_fk = {"player_id": player_id}

            # ── Player profile row ───────────────────────────────────────────
            player_records.append({**_scalars(player), **ctx})

            # ── Career ───────────────────────────────────────────────────────
            career = player.get("career") or {}
            if isinstance(career, dict):

                # careerStatsTotal
                career_total = career.get("careerStatsTotal") or {}
                if isinstance(career_total, dict) and career_total:
                    career_total_records.append({**_scalars(career_total), **player_fk, **ctx})

                # careerStatsAverage
                career_avg = career.get("careerStatsAverage") or {}
                if isinstance(career_avg, dict) and career_avg:
                    career_avg_records.append({**_scalars(career_avg), **player_fk, **ctx})

                # career → teams → team[]
                career_teams_node = career.get("teams") or {}
                if isinstance(career_teams_node, dict):
                    career_teams = _to_list(career_teams_node.get("team"))
                else:
                    career_teams = _to_list(career_teams_node)

                for team in career_teams:
                    if not isinstance(team, dict):
                        continue

                    team_fk = {
                        **player_fk,
                        "team_id":        team.get("teamId"),
                        "team_name":      team.get("teamName"),
                        "is_current_team": team.get("isCurrentTeam"),
                    }

                    ct = team.get("teamStatsTotal") or {}
                    if isinstance(ct, dict) and ct:
                        career_team_total_records.append({**_scalars(ct), **team_fk, **ctx})

                    ca = team.get("teamStatsAverage") or {}
                    if isinstance(ca, dict) and ca:
                        career_team_avg_records.append({**_scalars(ca), **team_fk, **ctx})

            # ── Season(s) ─────────────────────────────────────────────────────
            # Normally one <season> per player per ingestion; _to_list handles
            # both single-dict and multi-element cases safely.
            for season_elem in _to_list(player.get("season")):
                if not isinstance(season_elem, dict):
                    continue

                # seasonId is an attribute on the <season> element itself
                season_id_val = season_elem.get("seasonId") or season_id
                season_fk     = {**player_fk, "season_id": season_id_val}

                # seasonStatsTotal (scalar attrs only; roundStats excluded by _scalars)
                season_total = season_elem.get("seasonStatsTotal") or {}
                if isinstance(season_total, dict) and season_total:
                    season_total_records.append({**_scalars(season_total), **season_fk, **ctx})

                    # roundStats → round[]
                    round_stats = season_total.get("roundStats") or {}
                    if isinstance(round_stats, dict):
                        for round_rec in _to_list(round_stats.get("round")):
                            if not isinstance(round_rec, dict):
                                continue
                            round_records.append({**_scalars(round_rec), **season_fk, **ctx})

                # seasonStatsAverage
                season_avg = season_elem.get("seasonStatsAverage") or {}
                if isinstance(season_avg, dict) and season_avg:
                    season_avg_records.append({**_scalars(season_avg), **season_fk, **ctx})

                # season → teams → team[]
                season_teams_node = season_elem.get("teams") or {}
                if isinstance(season_teams_node, dict):
                    season_teams = _to_list(season_teams_node.get("team"))
                else:
                    season_teams = _to_list(season_teams_node)

                for team in season_teams:
                    if not isinstance(team, dict):
                        continue

                    s_team_fk = {
                        **season_fk,
                        "team_id":         team.get("teamId"),
                        "team_name":       team.get("teamName"),
                        "is_current_team": team.get("isCurrentTeam"),
                    }

                    st = team.get("teamStatsTotal") or {}
                    if isinstance(st, dict) and st:
                        season_team_total_records.append({**_scalars(st), **s_team_fk, **ctx})

                    sa = team.get("teamStatsAverage") or {}
                    if isinstance(sa, dict) and sa:
                        season_team_avg_records.append({**_scalars(sa), **s_team_fk, **ctx})

    # ── Write tables ──────────────────────────────────────────────────────────
    write_mode = config.get("write_mode", "overwrite")

    def _write(records, table_name):
        if not records:
            print(f"[playerstatsmaster] No records for {table_name} — skipping")
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
        print(f"[playerstatsmaster] {len(_rows)} rows -> {table_name} (mode={write_mode})")

    _write(root_records,              f"{target_prefix}")
    _write(player_records,            f"{target_prefix}_player")
    _write(career_total_records,      f"{target_prefix}_career_total")
    _write(career_avg_records,        f"{target_prefix}_career_average")
    _write(career_team_total_records, f"{target_prefix}_career_team_total")
    _write(career_team_avg_records,   f"{target_prefix}_career_team_avg")
    _write(season_total_records,      f"{target_prefix}_season_total")
    _write(season_avg_records,        f"{target_prefix}_season_average")
    _write(season_team_total_records, f"{target_prefix}_season_team_total")
    _write(season_team_avg_records,   f"{target_prefix}_season_team_avg")
    _write(round_records,             f"{target_prefix}_round")


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
PLAYERSTATSMASTER_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_prd.bronze_stats.historical_stats_by_round",
    "target_prefix": "nrl_datalakehouse_prd.bronze_stats_flatten.nrl_player_stats_master",

    # Always append so historical loads accumulate correctly.
    # The runner overrides this to "overwrite" only on the very first (full) load.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'historical_stats_by_round_{competition_id}_{season}%'",

    # All nested structures owned by post_run_fn — exclude from framework
    # auto-discovery to avoid partial / duplicate tables.
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
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "players": "players",
    },

    "dedup_keys_map": {
        "players": ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Suppress framework-generated root table for 'players'; all tables owned by post_run_fn.
    "exclude_root_aliases": {"players"},

    # Creates: nrl_player_stats_master,
    #          nrl_player_stats_master_player,
    #          nrl_player_stats_master_career_total,
    #          nrl_player_stats_master_career_average,
    #          nrl_player_stats_master_career_team_total,
    #          nrl_player_stats_master_career_team_avg,
    #          nrl_player_stats_master_season_total,
    #          nrl_player_stats_master_season_average,
    #          nrl_player_stats_master_season_team_total,
    #          nrl_player_stats_master_season_team_avg,
    #          nrl_player_stats_master_round
    "post_run_fn": _create_player_stats_master_tables,
}
