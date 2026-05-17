"""
NRL Team Stats API — Flattening Config
Source  : api/NRL/teamStats/{competitionId}/{seasonId}/{roundNumber}
Bronze  : nrl_datalakehouse_qa.bronze_stats.team_stats
Mode    : l0payload is an OBJECT (<teamStats> root attributes).
Elements: l1element = 'teams'                    — array of team objects
          l1element = 'competitionTeamAverageStats' — competition-level average stats

ingested_from pattern : team_stats_{competitionId}_{seasonId}_{roundNumber}
e.g. team_stats_111_2026_9

l0payload (object — <teamStats> root attributes):
  { competitionId, competitionName }

l1element = 'teams' payload — array of team objects:
  team { teamId, teamName, teamLogoURL, teamHexColour, teamHexColour2,
    currentRoundStats  { [flat attrs] },
    seasonStats        { [flat attrs] },
    seasonAverageStats { [flat attrs] },
    historicalStats    { [flat attrs] },
    historicalAverageStats { [flat attrs] }
  }

l1element = 'competitionTeamAverageStats' payload — flat stat attrs object:
  { seasonId, [flat stat attrs] }

Output tables (target_prefix = nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_stats):
  nrl_team_stats_teams   ← Level 0  Root (one row per ingestion)
  nrl_team_stats_team    ← Level 1  Team (one row per team per stat_type per ingestion)
                            stat_type ∈ { currentRoundStats, seasonStats,
                                          seasonAverageStats, historicalStats,
                                          historicalAverageStats }
  nrl_team_stats_comp_avg ← Level 0  Competition Average (one row per ingestion)

NOTE: all tables are created by post_run_fn.
      nrl_team_stats_team uses a stat_type discriminator column — one row per
      team per stat block — rather than separate tables or column prefixes.
      FK columns on child tables: competition_id, season_id, round_number, team_id only.
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
        print(f"[teamstats] No records for {table_name} — skipping")
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
    print(f"[teamstats] {len(rows)} rows → {table_name} (mode={write_mode})")


# ---------------------------------------------------------------------------
# post_run_fn
# ---------------------------------------------------------------------------
def _create_team_stats_tables(config, spark):
    """
    Creates three doc-aligned tables from the teamStats bronze data.

    nrl_team_stats_teams    — one row per ingestion (root context)
    nrl_team_stats_team     — one row per team per stat_type per ingestion
                              stat_type: currentRoundStats | seasonStats | seasonAverageStats
                                         | historicalStats | historicalAverageStats
    nrl_team_stats_comp_avg — one row per ingestion (competition-level average stats)
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

    root_records     = []
    team_records     = []
    comp_avg_records = []

    seen_root     = set()
    seen_comp_avg = set()

    for row in rows:
        # Parse context: team_stats_{competitionId}_{seasonId}_{roundNumber}
        parts          = row.ingested_from.split("_")
        competition_id = parts[2] if len(parts) > 2 else None
        season_id      = parts[3] if len(parts) > 3 else None
        round_number   = parts[4] if len(parts) > 4 else None

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
            "round_number":      round_number,
            "source_schema":     "bronze_stats",
            "source_table_name": "team_stats",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Level 0: Root row — one per ingestion ─────────────────────────────
        root_key = (competition_id, season_id, round_number)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name":  competition_name,
                "target_table_name": "nrl_team_stats_teams",
            })

        # ── Competition average — own l1element row ───────────────────────────
        if row.l1element == "competitionTeamAverageStats":
            comp_avg_key = (competition_id, season_id, round_number)
            if comp_avg_key not in seen_comp_avg:
                seen_comp_avg.add(comp_avg_key)
                payload = _json.loads(row.p) if row.p else {}
                if isinstance(payload, dict):
                    comp_avg_records.append({
                        **_scalars(payload),
                        **ctx,
                        "target_table_name": "nrl_team_stats_comp_avg",
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

            team_id  = team.get("teamId")
            team_ctx = {
                **ctx,
                "team_id": team_id,
            }

            # ── Level 1: one row per team per stat_type ───────────────────────
            # nrl_team_stats_team is the sole team-data table in this config so
            # team identity attrs (name, logo, colours) are included here too.
            for stat_type, node in [
                ("currentRoundStats",      team.get("currentRoundStats")),
                ("seasonStats",            team.get("seasonStats")),
                ("seasonAverageStats",     team.get("seasonAverageStats")),
                ("historicalStats",        team.get("historicalStats")),
                ("historicalAverageStats", team.get("historicalAverageStats")),
            ]:
                if not isinstance(node, dict) or not node:
                    continue
                team_records.append({
                    **_scalars(node),
                    **team_ctx,
                    "team_name":         team.get("teamName"),
                    "team_logo_url":     team.get("teamLogoURL"),
                    "team_hex_colour":   team.get("teamHexColour"),
                    "team_hex_colour2":  team.get("teamHexColour2"),
                    "stat_type":         stat_type,
                    "target_table_name": "nrl_team_stats_team",
                })

    write_mode = config.get("write_mode", "overwrite")
    _write_records(root_records,     f"{prefix}_teams",    spark, write_mode)
    _write_records(team_records,     f"{prefix}_team",     spark, write_mode)
    _write_records(comp_avg_records, f"{prefix}_comp_avg", spark, write_mode)


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
TEAMSTATS_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_qa.bronze_stats.team_stats",
    "target_prefix": "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_stats",

    # Always append so historical loads accumulate correctly.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'team_stats_{competition_id}_{season}%'",

    # All nested structures handled by post_run_fn.
    "exclude_fields": {
        "team",
        "currentRoundStats",
        "seasonStats",
        "seasonAverageStats",
        "historicalStats",
        "historicalAverageStats",
        "competitionTeamAverageStats",
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

    # Creates: nrl_team_stats_teams,
    #          nrl_team_stats_team   (stat_type discriminator),
    #          nrl_team_stats_comp_avg
    "post_run_fn": _create_team_stats_tables,
}
