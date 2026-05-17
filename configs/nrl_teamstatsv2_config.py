"""
NRL Team Stats V2 API — Flattening Config
Source  : api/NRL/teamStatsV2/{competitionId}/{seasonId}
Bronze  : nrl_datalakehouse_qa.bronze_stats.team_stats_v2
Mode    : l0payload is an OBJECT (<teamStatsV2> root attributes).
Elements: l1element = 'teams'                  — array of team objects
          l1element = 'competitionTeamAverageStats' — competition-level average stats

ingested_from pattern : team_stats_v2_{competitionId}_{seasonId}
e.g. team_stats_v2_111_2026

l0payload (object — <teamStatsV2> root attributes):
  { competitionId, competitionName, seasonId }

l1element = 'teams' payload — array of team objects:
  team { teamId, teamName,
    seasonStats { [flat attrs],
      rounds { round [ { roundNumber, gameID, oppositionId, [flat attrs] } ] }
    },
    seasonAverageStats  { [flat attrs] },
    historicalStats     { [flat attrs] },
    historicalAverageStats { [flat attrs] }
  }

l1element = 'competitionTeamAverageStats' payload — flat stat attrs object:
  { [flat stat attrs] }

Output tables (target_prefix = nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_stats_v2):
  nrl_team_stats_v2                       ← Level 0  Root (one row per competition+season)
  nrl_team_stats_v2_competition_average   ← Level 0b Competition Average (one row per competition+season)
  nrl_team_stats_v2_team                  ← Level 1  Team (one row per team)
  nrl_team_stats_v2_season_total          ← Level 2a Season Total (one row per team per season)
  nrl_team_stats_v2_season_average        ← Level 2b Season Average (one row per team per season)
  nrl_team_stats_v2_historical_total      ← Level 2c Historical Total (one row per team)
  nrl_team_stats_v2_historical_average    ← Level 2d Historical Average (one row per team)
  nrl_team_stats_v2_round                 ← Level 3  Round (one row per round per team)

NOTE: all tables are created by post_run_fn.
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
        print(f"[teamstatsv2] No records for {table_name} — skipping")
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
    print(f"[teamstatsv2] {len(rows)} rows → {table_name} (mode={write_mode})")


# ---------------------------------------------------------------------------
# post_run_fn
# ---------------------------------------------------------------------------
def _create_team_stats_v2_tables(config, spark):
    """
    Creates eight doc-aligned tables from the teamStatsV2 bronze data.

    nrl_team_stats_v2                     — one row per competition+season (root context)
    nrl_team_stats_v2_competition_average — one row per competition+season (comp-level avg stats)
    nrl_team_stats_v2_team                — one row per team (team identity)
    nrl_team_stats_v2_season_total        — one row per team per season (seasonStats scalars)
    nrl_team_stats_v2_season_average      — one row per team per season (seasonAverageStats scalars)
    nrl_team_stats_v2_historical_total    — one row per team (historicalStats scalars)
    nrl_team_stats_v2_historical_average  — one row per team (historicalAverageStats scalars)
    nrl_team_stats_v2_round               — one row per round per team (seasonStats.rounds.round[])
    """
    bronze = config["bronze_table"]
    prefix = config["target_prefix"]
    bf     = config.get("bronze_filter")

    bronze_df = spark.table(bronze)
    if bf:
        bronze_df = bronze_df.filter(bf)

    rows = (
        bronze_df
        .filter(F.col("l1element").isin("teams", "competitionTeamAverageStats"))
        .select(
            "ingested_from",
            "ingested_at",
            "l1element",
            F.col("payload").cast("string").alias("p"),
            F.col("l0payload").cast("string").alias("l0"),
        )
        .collect()
    )

    root_records      = []
    comp_avg_records  = []
    team_records      = []
    season_total_recs = []
    season_avg_recs   = []
    hist_total_recs   = []
    hist_avg_recs     = []
    round_records     = []

    seen_root     = set()
    seen_comp_avg = set()
    seen_team     = set()

    for row in rows:
        # Parse context: team_stats_v2_{competitionId}_{seasonId}
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
            "source_table_name": "team_stats_v2",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Level 0: Root row — one per competition+season ────────────────────
        root_key = (competition_id, season_id)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name":  competition_name,
                "target_table_name": "nrl_team_stats_v2",
            })

        # ── Level 0b: Competition average — own l1element row ────────────────
        if row.l1element == "competitionTeamAverageStats":
            comp_avg_key = (competition_id, season_id)
            if comp_avg_key not in seen_comp_avg:
                seen_comp_avg.add(comp_avg_key)
                payload = _json.loads(row.p) if row.p else {}
                if isinstance(payload, dict):
                    comp_avg_records.append({
                        **_scalars(payload),
                        **ctx,
                        "target_table_name": "nrl_team_stats_v2_competition_average",
                    })
            continue   # nothing else to process for this l1element

        # ── Teams (l1element == 'teams') ──────────────────────────────────────
        payload = _json.loads(row.p) if row.p else []
        if isinstance(payload, dict):
            teams = _to_list(payload.get("team") or payload)
        else:
            teams = _to_list(payload)

        for team in teams:
            if not isinstance(team, dict):
                continue

            team_id   = team.get("teamId")
            team_name = team.get("teamName")

            team_ctx = {
                **ctx,
                "team_id": team_id,
            }

            # ── Level 1: Team row — one per team ──────────────────────────────
            team_key = (competition_id, team_id)
            if team_key not in seen_team:
                seen_team.add(team_key)
                team_records.append({
                    **team_ctx,
                    "team_name":         team_name,
                    "target_table_name": "nrl_team_stats_v2_team",
                })

            # ── Level 2a: Season total (seasonStats scalars, rounds excluded) ─
            season_node = team.get("seasonStats") or {}
            if isinstance(season_node, dict):
                season_total_recs.append({
                    **_scalars(season_node),   # excludes nested 'rounds' dict automatically
                    **team_ctx,
                    "target_table_name": "nrl_team_stats_v2_season_total",
                })

                # ── Level 3: Round rows — one per round per team ───────────────
                rounds_node = season_node.get("rounds") or {}
                round_list  = (
                    _to_list(rounds_node.get("round"))
                    if isinstance(rounds_node, dict)
                    else _to_list(rounds_node)
                )
                for rd in round_list:
                    if not isinstance(rd, dict):
                        continue
                    round_records.append({
                        **_scalars(rd),
                        **team_ctx,
                        "target_table_name": "nrl_team_stats_v2_round",
                    })

            # ── Level 2b: Season average ──────────────────────────────────────
            season_avg_node = team.get("seasonAverageStats") or {}
            if isinstance(season_avg_node, dict) and season_avg_node:
                season_avg_recs.append({
                    **_scalars(season_avg_node),
                    **team_ctx,
                    "target_table_name": "nrl_team_stats_v2_season_average",
                })

            # ── Level 2c: Historical total ────────────────────────────────────
            hist_node = team.get("historicalStats") or {}
            if isinstance(hist_node, dict) and hist_node:
                hist_total_recs.append({
                    **_scalars(hist_node),
                    **team_ctx,
                    "target_table_name": "nrl_team_stats_v2_historical_total",
                })

            # ── Level 2d: Historical average ──────────────────────────────────
            hist_avg_node = team.get("historicalAverageStats") or {}
            if isinstance(hist_avg_node, dict) and hist_avg_node:
                hist_avg_recs.append({
                    **_scalars(hist_avg_node),
                    **team_ctx,
                    "target_table_name": "nrl_team_stats_v2_historical_average",
                })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,      f"{prefix}",                     spark, write_mode)
    _write_records(comp_avg_records,  f"{prefix}_competition_average", spark, write_mode)
    _write_records(team_records,      f"{prefix}_team",                spark, write_mode)
    _write_records(season_total_recs, f"{prefix}_season_total",        spark, write_mode)
    _write_records(season_avg_recs,   f"{prefix}_season_average",      spark, write_mode)
    _write_records(hist_total_recs,   f"{prefix}_historical_total",    spark, write_mode)
    _write_records(hist_avg_recs,     f"{prefix}_historical_average",  spark, write_mode)
    _write_records(round_records,     f"{prefix}_round",               spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
TEAMSTATSV2_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_qa.bronze_stats.team_stats_v2",
    "target_prefix": "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_stats_v2",

    # Always append so historical loads accumulate correctly.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'team_stats_v2_{competition_id}_{season}%'",

    # All nested structures handled by post_run_fn.
    "exclude_fields": {
        "team",
        "seasonStats",
        "seasonAverageStats",
        "historicalStats",
        "historicalAverageStats",
        "competitionTeamAverageStats",
        "rounds",
        "round",
    },

    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "teams":                      "teams",
        "competitionTeamAverageStats": "competitionTeamAverageStats",
    },

    "dedup_keys_map": {
        "teams":                      ["ingested_from"],
        "competitionTeamAverageStats": ["ingested_from"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # Suppress framework-generated tables; all tables owned by post_run_fn.
    "exclude_root_aliases": {"teams", "competitionTeamAverageStats"},

    # Creates: nrl_team_stats_v2,
    #          nrl_team_stats_v2_competition_average,
    #          nrl_team_stats_v2_team,
    #          nrl_team_stats_v2_season_total,
    #          nrl_team_stats_v2_season_average,
    #          nrl_team_stats_v2_historical_total,
    #          nrl_team_stats_v2_historical_average,
    #          nrl_team_stats_v2_round
    "post_run_fn": _create_team_stats_v2_tables,
}
