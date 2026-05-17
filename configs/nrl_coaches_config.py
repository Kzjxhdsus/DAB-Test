"""
NRL Coaches API — Flattening Config
Source  : api/NRL/coaches/{competitionId}/{seasonId}
Bronze  : nrl_datalakehouse_qa.bronze_stats.coaches
Mode    : l0payload is an OBJECT (<coaches> root attributes);
          payload per l1element is the teams array.
Elements: l1element = 'teams'

ingested_from pattern : coaches_{competitionId}_{seasonId}
e.g. coaches_111_2026

l0payload (object — <coaches> root attributes):
  { competitionId, competitionName, seasonId }

l1element = 'teams' payload — array of team objects:
  team { teamId, teamName,
    coaches {
      coach [ {
        fullName, coachNickName, coachId, dob,
        headshotURL, bodyshotURL, birthplace,
        roleId, roleName, activeCoach,
        careerCoachingDebut, careerGamesCoached, careerWins, careerLosses,
        careerDraws, careerWinPercentage, careerPointsFor, careerPointsAgainst,
        teamCoachingDebut,  teamGamesCoached,  teamWins,  teamLosses,
        teamDraws,  teamWinPercentage,  teamPointsFor,  teamPointsAgainst,
        seasonGamesCoached, seasonWins, seasonLosses, seasonDraws,
        seasonWinPercentage, seasonPointsFor, seasonPointsAgainst,
        longestWinningStreak, longestLosingStreak
      } ]
    }
  }

Note: a team may have more than one coach row (e.g. mid-season coaching change —
both the departing and incoming coach appear with activeCoach="false"/"true").

Output tables (target_prefix = nrl_datalakehouse_qa.bronze_stats_flatten.nrl_coaches):
  nrl_coaches        ← root (one row per competition+season ingestion)
  nrl_coaches_coach  ← one row per coach per team

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
# post_run_fn
# ---------------------------------------------------------------------------
def _create_coaches_tables(config, spark):
    """
    Creates two doc-aligned tables from the coaches bronze data.

    nrl_coaches       — one row per competition+season (root context)
    nrl_coaches_coach — one row per coach per team (all coach attrs + team FK + ctx)
    """
    bronze        = config["bronze_table"]
    target_prefix = config["target_prefix"]

    _bronze_df = spark.table(bronze)
    _bf = config.get("bronze_filter")
    if _bf:
        _bronze_df = _bronze_df.filter(_bf)

    rows = (
        _bronze_df
        .filter(F.col("l1element") == "teams")
        .select(
            F.col("l0payload").cast("string").alias("l0"),
            F.col("payload").cast("string").alias("p"),
            "ingested_from",
            "ingested_at",
        )
        .collect()
    )

    root_records  = []
    coach_records = []
    seen_root     = set()

    for row in rows:
        ingested_from = row.ingested_from
        ingested_at   = str(row.ingested_at)

        # Parse context: coaches_{competitionId}_{seasonId}
        parts          = ingested_from.split("_")
        competition_id = parts[1] if len(parts) > 1 else None
        season_id      = parts[2] if len(parts) > 2 else None

        # Extract competition_name from l0payload
        competition_name = None
        if row.l0:
            try:
                l0 = _json.loads(row.l0)
                if isinstance(l0, dict):
                    competition_name = l0.get("competitionName")
            except Exception:
                pass

        ctx = {
            "ingested_from":     ingested_from,
            "ingested_at":       ingested_at,
            "competition_id":    competition_id,
            "season_id":         season_id,
            "source_schema":     "bronze_stats",
            "source_table_name": "coaches",
            "target_schema":     "bronze_stats_flatten",
        }

        # ── Root row — one per competition+season ─────────────────────────────
        root_key = (competition_id, season_id)
        if root_key not in seen_root:
            seen_root.add(root_key)
            root_records.append({
                **ctx,
                "competition_name":  competition_name,
                "target_table_name": "nrl_coaches",
            })

        payload = _json.loads(row.p) if row.p else []

        # Normalise: XML converters may wrap as {"team": [...]}
        if isinstance(payload, dict):
            teams = _to_list(payload.get("team") or payload)
        else:
            teams = _to_list(payload)

        # ── teams → coaches → coach[] ─────────────────────────────────────────
        for team in teams:
            if not isinstance(team, dict):
                continue

            team_id   = team.get("teamId")
            team_name = team.get("teamName")

            coaches_node = team.get("coaches") or {}
            if isinstance(coaches_node, dict):
                coach_list = _to_list(coaches_node.get("coach"))
            else:
                coach_list = _to_list(coaches_node)

            for coach in coach_list:
                if not isinstance(coach, dict):
                    continue
                coach_records.append({
                    **_scalars(coach),
                    "team_id":   team_id,
                    "team_name": team_name,
                    **ctx,
                    "target_table_name": "nrl_coaches_coach",
                })

    # ── Write tables ──────────────────────────────────────────────────────────
    write_mode = config.get("write_mode", "overwrite")

    def _write(records, table_name):
        if not records:
            print(f"[coaches] No records for {table_name} — skipping")
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
        print(f"[coaches] {len(_rows)} rows -> {table_name} (mode={write_mode})")

    _write(root_records,  f"{target_prefix}")
    _write(coach_records, f"{target_prefix}_coach")


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
COACHES_CONFIG = {
    "bronze_table":  "nrl_datalakehouse_qa.bronze_stats.coaches",
    "target_prefix": "nrl_datalakehouse_qa.bronze_stats_flatten.nrl_coaches",

    # Always append so historical loads accumulate correctly.
    "write_mode": "append",

    "source_filter_template": "ingested_from LIKE 'coaches_{competition_id}_{season}%'",

    # All nested structures handled by post_run_fn.
    "exclude_fields": {
        "team",
        "coaches",
        "coach",
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

    # Suppress framework-generated root table; all tables owned by post_run_fn.
    "exclude_root_aliases": {"teams"},

    # Creates: nrl_coaches,
    #          nrl_coaches_coach
    "post_run_fn": _create_coaches_tables,
}
