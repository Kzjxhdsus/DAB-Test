"""
NRL Season List API — Flattening Config
Source : nrl_datalakehouse_prd.bronze_stats.season_list
Bronze : L0 mode — l0payload holds competition-level scalars
         (competitionId, competitionName, etc.).
         l1element is always "seasons" (one row per season).
         payload is a DICT containing:
           - season scalars : active, currentSeason, id, name
           - clubs     : {"club":     [...]}   ← wrapper dict
           - coaches   : {"coach":    [...]}   ← wrapper dict
           - officials : {"official": [...]}   ← wrapper dict
           - rounds    : {"round":    [...]}   ← wrapper dict
           - venues    : {"venue":    [...]}   ← wrapper dict

clubs/coaches/officials/rounds/venues are NOT separate l1elements — they are
nested inside the seasons payload.  The post_run_fn navigates one level deeper
(.club / .coach / etc.) and writes each child table directly.

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.season_list):
  nrl_season_season    ← one row per season   (from framework via l1element)
  nrl_season_round     ← rounds per season    (post_run_fn)
  nrl_season_club      ← clubs/teams          (post_run_fn)
  nrl_season_coach     ← coaches              (post_run_fn)
  nrl_season_venue     ← venues               (post_run_fn)
  nrl_season_official  ← officials            (post_run_fn)
"""

import json as _json
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# post_run_fn: navigate wrapper dicts and write child tables
# ---------------------------------------------------------------------------
def _create_season_list_tables(config, spark):
    """
    Reads the bronze season_list table and navigates each wrapper dict:
      payload.rounds.round[]       → round rows
      payload.clubs.club[]         → club rows
      payload.coaches.coach[]      → coach rows
      payload.venues.venue[]       → venue rows
      payload.officials.official[] → official rows
    """
    bronze        = config["bronze_table"]
    target_prefix = config["target_prefix"]
    write_mode    = config.get("write_mode", "overwrite")

    _bronze_df = spark.table(bronze)  # noqa: F821
    _bf = config.get("bronze_filter")
    if _bf:
        _bronze_df = _bronze_df.filter(_bf)

    rows = (
        _bronze_df
        .select(
            F.col("l0payload").cast("string").alias("l0"),
            F.col("payload").cast("string").alias("p"),
            "ingested_from",
            "ingested_at",
        )
        .collect()
    )

    print(f"[season_list post_run_fn] {len(rows)} bronze rows to process")

    round_records    = []
    club_records     = []
    coach_records    = []
    venue_records    = []
    official_records = []

    for row in rows:
        l0      = _json.loads(row.l0) if row.l0 else {}
        payload = _json.loads(row.p)  if row.p  else {}

        if not isinstance(payload, dict):
            print(f"[season_list post_run_fn] WARNING: unexpected payload type "
                  f"{type(payload)} for ingested_from={row.ingested_from} — skipping")
            continue

        # Season identity — carried as foreign key on every child row
        season_id = payload.get("id")

        # ── Shared helper: unwrap {"key": item_or_list} → normalised list ─────
        def _unwrap(wrapper_key, item_key):
            wrapper = payload.get(wrapper_key) or {}
            if not isinstance(wrapper, dict):
                return []
            items = wrapper.get(item_key, [])
            if isinstance(items, dict):
                items = [items]   # single-element XML conversion
            return [i for i in items if isinstance(i, dict)]

        # ── Rounds ────────────────────────────────────────────────────────────
        for round_ in _unwrap("rounds", "round"):
            rec = {k: v for k, v in round_.items() if not isinstance(v, (dict, list))}
            rec.update({"season_id": season_id, "ingested_from": row.ingested_from})
            round_records.append(rec)

        # ── Clubs (teams) ─────────────────────────────────────────────────────
        for club in _unwrap("clubs", "club"):
            rec = {k: v for k, v in club.items() if not isinstance(v, (dict, list))}
            rec.update({"season_id": season_id, "ingested_from": row.ingested_from})
            club_records.append(rec)

        # ── Coaches ───────────────────────────────────────────────────────────
        for coach in _unwrap("coaches", "coach"):
            rec = {k: v for k, v in coach.items() if not isinstance(v, (dict, list))}
            rec.update({"season_id": season_id, "ingested_from": row.ingested_from})
            coach_records.append(rec)

        # ── Venues ────────────────────────────────────────────────────────────
        for venue in _unwrap("venues", "venue"):
            rec = {k: v for k, v in venue.items() if not isinstance(v, (dict, list))}
            rec.update({"season_id": season_id, "ingested_from": row.ingested_from})
            venue_records.append(rec)

        # ── Officials ─────────────────────────────────────────────────────────
        for official in _unwrap("officials", "official"):
            rec = {k: v for k, v in official.items() if not isinstance(v, (dict, list))}
            rec.update({"season_id": season_id, "ingested_from": row.ingested_from})
            official_records.append(rec)

    # ── Write tables ──────────────────────────────────────────────────────────
    def _write(records, table_name):
        if not records:
            print(f"[season_list post_run_fn] No records for {table_name} — skipping")
            return
        (
            spark.createDataFrame(records)
            .write.format("delta")
            .mode(write_mode)
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        print(f"[season_list post_run_fn] Written {len(records)} rows → {table_name}  (mode={write_mode})")

    _write(round_records,    f"{target_prefix}_round")
    _write(club_records,     f"{target_prefix}_club")
    _write(coach_records,    f"{target_prefix}_coach")
    _write(venue_records,    f"{target_prefix}_venue")
    _write(official_records, f"{target_prefix}_official")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SEASON_LIST_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.season_list",      # ← verify
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.season_list",

    # ingested_from format: season_list_{competitionId}_{season}
    # One row per season — include {season} so each season has its own watermark.
    "source_filter_template": "ingested_from = 'season_list_{competition_id}'",

    # Prevent the framework from auto-discovering the wrapper dicts as child tables.
    # The post_run_fn navigates one level deeper (.round / .club / etc.).
    "exclude_fields": {"rounds", "coaches", "clubs", "venues", "officials"},

    "flatten_structs":    {},
    "force_array_fields": set(),

    # Only one l1element: "seasons" (one bronze row per season).
    # Framework produces nrl_season_season with season-level scalars
    # (active, currentSeason, id, name).
    "elements": {
        "seasons": "season",
    },

    "dedup_keys_map": {
        "season":   ["id"],          # id = season year e.g. "2026"
        "round":    ["id"],          # id = round id   e.g. "11561"
        "club":     ["team_id"],
        "coach":    ["coach_id"],
        "venue":    ["venue_id"],
        "official": ["official_id"],
    },

    "sample_size":    200,
    "type_overrides": {},

    "post_run_fn": _create_season_list_tables,
}
