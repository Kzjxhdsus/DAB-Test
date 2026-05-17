"""
NRL Round Schedule API — Flattening Config
Source : nrl_datalakehouse_prd.bronze_stats.round_fixtures
Bronze : L0 mode — l0payload holds competition/season-level scalars
         (competitionId, competitionName, seasonId, etc.).
         payload is a DICT per round containing:
           - round-level scalars (roundId, roundName, roundAbbreviation, etc.)
           - gameFixtures : {"gameFixture": [{...}, ...]}  ← one or more games
           - teamByes     : {"teamBye": {...} or [{...}]}  ← zero or more byes

Only one l1element exists in the bronze table: "roundFixtures" (one row per round).
gameFixtures and teamByes are NOT separate l1elements — they are nested inside
the roundFixtures payload.  The framework cannot auto-discover them so a
post_run_fn navigates the nested structure and writes the child tables directly.

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.round_fixtures):
  nrl_roundschedule_roundfixtures                       ← one row per round
  nrl_roundschedule_gamefixture                         ← one row per game      (post_run_fn)
  nrl_roundschedule_gamefixture_team                    ← two rows per game     (post_run_fn)
  nrl_roundschedule_gamefixture_region                  ← broadcast regions     (post_run_fn)
  nrl_roundschedule_gamefixture_region_broadcaster      ← individual broadcasters (post_run_fn)
  nrl_roundschedule_teambye                             ← bye teams per round   (post_run_fn)
"""

import json as _json
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# post_run_fn: navigate nested gameFixtures / teamByes and write child tables
# ---------------------------------------------------------------------------
def _create_roundschedule_game_tables(config, spark):
    """
    Reads the bronze round_fixtures table and navigates the nested structure:
      payload.gameFixtures.gameFixture[]      → gamefixture rows
      payload.gameFixtures.gameFixture[].teams.team[]             → team rows
      payload.gameFixtures.gameFixture[].broadcastInfo.regions.region[] → region rows
      payload.gameFixtures.gameFixture[].broadcastInfo.regions.region[].broadcasters.broadcaster[] → broadcaster rows
      payload.teamByes.teamBye (dict or list)  → teambye rows
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

    print(f"[roundschedule post_run_fn] {len(rows)} bronze rows to process")

    game_records        = []
    team_records        = []
    region_records      = []
    broadcaster_records = []
    bye_records         = []

    for row in rows:
        l0      = _json.loads(row.l0) if row.l0 else {}
        payload = _json.loads(row.p)  if row.p  else {}

        # Payload should be a dict — skip malformed rows
        if not isinstance(payload, dict):
            print(f"[roundschedule post_run_fn] WARNING: unexpected payload type "
                  f"{type(payload)} for ingested_from={row.ingested_from} — skipping")
            continue

        # Round-level scalars come from the payload dict (not l0payload)
        round_id   = payload.get("roundId")
        round_name = payload.get("roundName")
        round_abbr = payload.get("roundAbbreviation")

        # ── gameFixtures ──────────────────────────────────────────────────────
        gf_wrapper = payload.get("gameFixtures") or {}
        if isinstance(gf_wrapper, dict):
            gf_raw = gf_wrapper.get("gameFixture", [])
        else:
            gf_raw = gf_wrapper   # already a list (defensive)

        # Normalise single-game rounds: XML converter returns a dict not a list
        if isinstance(gf_raw, dict):
            gf_raw = [gf_raw]

        for gf in gf_raw:
            if not isinstance(gf, dict):
                continue

            game_id = gf.get("gameId")

            # Scalar game fields (exclude nested dicts/lists)
            game_row = {k: v for k, v in gf.items() if not isinstance(v, (dict, list))}
            game_row.update({
                "round_id":           round_id,
                "round_name":         round_name,
                "round_abbreviation": round_abbr,
                "ingested_from":      row.ingested_from,
                "ingested_at":        str(row.ingested_at),
            })
            game_records.append(game_row)

            # ── Teams (teams.team — single dict or list) ──────────────────────
            raw_teams = gf.get("teams") or {}
            teams = raw_teams.get("team", []) if isinstance(raw_teams, dict) else []
            if isinstance(teams, dict):
                teams = [teams]
            for team in teams:
                if not isinstance(team, dict):
                    continue
                team_row = {k: v for k, v in team.items() if not isinstance(v, (dict, list))}
                team_row.update({
                    "game_id":       game_id,
                    "ingested_from": row.ingested_from,
                })
                team_records.append(team_row)

            # ── Broadcast regions (broadcastInfo.regions.region) ──────────────
            bi          = gf.get("broadcastInfo") or {}
            regions_raw = bi.get("regions") or {} if isinstance(bi, dict) else {}
            regions     = regions_raw.get("region", []) if isinstance(regions_raw, dict) else []
            if isinstance(regions, dict):
                regions = [regions]
            for region in regions:
                if not isinstance(region, dict):
                    continue
                region_id  = region.get("regionid")
                region_row = {k: v for k, v in region.items() if not isinstance(v, (dict, list))}
                region_row.update({
                    "game_id":       game_id,
                    "ingested_from": row.ingested_from,
                })
                region_records.append(region_row)

                # ── Broadcasters (broadcasters.broadcaster — single or list) ──
                bcs_raw      = region.get("broadcasters") or {}
                broadcasters = bcs_raw.get("broadcaster", []) if isinstance(bcs_raw, dict) else []
                if isinstance(broadcasters, dict):
                    broadcasters = [broadcasters]
                for bc in broadcasters:
                    if isinstance(bc, dict):
                        bc_row = dict(bc)
                    else:
                        bc_row = {"broadcaster": str(bc)}
                    bc_row.update({
                        "game_id":       game_id,
                        "region_id":     region_id,
                        "ingested_from": row.ingested_from,
                    })
                    broadcaster_records.append(bc_row)

        # ── teamByes ──────────────────────────────────────────────────────────
        tb_wrapper = payload.get("teamByes") or {}
        if isinstance(tb_wrapper, dict):
            tb_raw = tb_wrapper.get("teamBye", [])
        else:
            tb_raw = tb_wrapper   # already a list (defensive)

        # Normalise single-bye rounds: XML converter returns a dict not a list
        if isinstance(tb_raw, dict):
            tb_raw = [tb_raw]

        for tb in tb_raw:
            if not isinstance(tb, dict):
                continue
            bye_row = {k: v for k, v in tb.items() if not isinstance(v, (dict, list))}
            bye_row.update({
                "round_id":      round_id,
                "round_name":    round_name,
                "ingested_from": row.ingested_from,
                "ingested_at":   str(row.ingested_at),
            })
            bye_records.append(bye_row)

    # ── Write tables ──────────────────────────────────────────────────────────
    def _write(records, table_name):
        if not records:
            print(f"[roundschedule post_run_fn] No records for {table_name} — skipping")
            return
        (
            spark.createDataFrame(records)
            .write.format("delta")
            .mode(write_mode)
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        print(f"[roundschedule post_run_fn] Written {len(records)} rows → {table_name}  (mode={write_mode})")

    _write(game_records,        f"{target_prefix}_gamefixture")
    _write(team_records,        f"{target_prefix}_gamefixture_team")
    _write(region_records,      f"{target_prefix}_gamefixture_region")
    _write(broadcaster_records, f"{target_prefix}_gamefixture_region_broadcaster")
    _write(bye_records,         f"{target_prefix}_teambye")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ROUNDSCHEDULE_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.round_fixtures",      # ← verify
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.round_fixtures",

    # ingested_from format: round_fixtures_{competitionId}_{seasonId}_*
    "source_filter_template": "ingested_from LIKE 'round_fixtures_{competition_id}_{season}%'",

    # Prevent the framework from auto-discovering gameFixtures and teamByes as
    # child tables — they are wrapper dicts, not direct arrays.  The post_run_fn
    # handles them correctly by navigating one level deeper (.gameFixture / .teamBye).
    "exclude_fields": {"gameFixtures", "teamByes", "radioNetwork"},

    "flatten_structs":    {},
    "force_array_fields": set(),

    # Only one l1element exists in the bronze table.
    # The framework creates nrl_roundschedule_roundfixtures with round-level scalars.
    "elements": {
        "roundFixtures": "roundfixtures",
    },

    "dedup_keys_map": {
        "roundfixtures": ["round_id"],
    },

    "sample_size":    200,
    "type_overrides": {},

    # post_run_fn creates the game, team, region, broadcaster, and bye tables
    # by navigating the nested payload structure that the framework cannot handle.
    "post_run_fn": _create_roundschedule_game_tables,
}
