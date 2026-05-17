# Databricks notebook source
"""
nrl_pipeline_test_cases.py
==========================
SQL-based test suite for the NRL Bronze → Silver flattening pipeline.

Purpose
-------
Validates that the flattening pipeline produced correct, complete, and
deduplicated output in the silver layer.  Run this notebook manually after
a pipeline job or include it as a final step in the workflow.

Test groups
-----------
  TC-B  Bronze layer health        — source tables readable and non-empty
  TC-P  Pipeline execution         — audit log completeness, error log clean
  TC-S  Silver row counts          — every silver table has > 0 rows
  TC-D  Deduplication integrity    — no duplicate rows per dedup key
  TC-N  Null key column check      — primary key columns contain no NULLs
  TC-Q  Config-specific quality    — config-level business rules
  TC-I  Incremental integrity      — watermark bookkeeping is consistent

How to use
----------
1. Set the widgets at the top of the notebook (competition_id, season,
   audit_log_table, error_log_table) to match the job run you want to test.
2. Run All.
3. The final cell prints a summary table of PASS / FAIL / WARN per test.

Adding new tests
----------------
Call one of the helpers:
  _check(tc_id, description, sql, expect_zero=True)
    → PASS when COUNT(*) == 0  (default — used for "no bad rows" checks)
    → PASS when COUNT(*) >  0  when expect_zero=False
  _check_value(tc_id, description, sql, column, expected_value)
    → PASS when the returned scalar equals expected_value
"""

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# PARAMETERS
# Run this cell first — fill in the widgets to target a specific pipeline run.
# ──────────────────────────────────────────────────────────────────────────────

# Set default values here; override via Databricks widgets at runtime.
try:
    COMPETITION_ID  = dbutils.widgets.get("competition_id").strip()   # noqa: F821
except Exception:
    COMPETITION_ID  = "111"

try:
    SEASON          = dbutils.widgets.get("season").strip()           # noqa: F821
except Exception:
    SEASON          = "2026"

try:
    AUDIT_LOG_TABLE = dbutils.widgets.get("audit_log_table").strip()  # noqa: F821
except Exception:
    AUDIT_LOG_TABLE = "sandbox.ashish.nrl_pipeline_audit_log"

try:
    ERROR_LOG_TABLE = dbutils.widgets.get("error_log_table").strip()  # noqa: F821
except Exception:
    ERROR_LOG_TABLE = "sandbox.ashish.nrl_pipeline_error_log"

print(f"Competition : {COMPETITION_ID}")
print(f"Season      : {SEASON}")
print(f"Audit table : {AUDIT_LOG_TABLE}")
print(f"Error table : {ERROR_LOG_TABLE}")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# SILVER TABLE REGISTRY
#
# Maps each silver table to the column(s) that form its natural/dedup key.
# Used by TC-D (dedup) and TC-N (null key) test groups.
#
# Format:
#   "<catalog.schema.table>": {
#       "key_cols"   : ["col1", "col2"],   ← dedup key combination
#       "config"     : "config_name",       ← which job config produces it
#       "description": "human-readable",
#   }
# ──────────────────────────────────────────────────────────────────────────────

SILVER_TABLES = {

    # ── ladder ────────────────────────────────────────────────────────────────
    "sandbox.ashish.nrl_ladder_ladderposition": {
        "key_cols":    ["competition_id", "season_id", "team_id"],
        "config":      "ladder",
        "description": "Ladder positions",
    },

    # ── live_ladder ───────────────────────────────────────────────────────────
    "sandbox.ashish.nrl_live_ladder_ladderposition": {
        "key_cols":    ["competition_id", "season_id", "team_id"],
        "config":      "live_ladder",
        "description": "Live ladder positions",
    },

    # ── player_profile ────────────────────────────────────────────────────────
    "sandbox.ashish.nrl_playerprofile_player": {
        "key_cols":    ["player_id"],
        "config":      "player_profile",
        "description": "Player profile root",
    },
    "sandbox.ashish.nrl_playerprofile_team": {
        "key_cols":    ["player_id", "team_id"],
        "config":      "player_profile",
        "description": "Teams per player",
    },

    # ── player_associations ───────────────────────────────────────────────────
    "sandbox.ashish.nrl_player_associations_players": {
        "key_cols":    ["ingested_from"],
        "config":      "player_associations",
        "description": "Player-associations root",
    },
    "sandbox.ashish.nrl_player_associations_player": {
        "key_cols":    ["player_id"],
        "config":      "player_associations",
        "description": "Individual player associations",
    },
    "sandbox.ashish.nrl_player_associations_competition": {
        "key_cols":    ["ingested_from"],
        "config":      "player_associations",
        "description": "Competitions in player associations",
    },
    "sandbox.ashish.nrl_player_associations_season": {
        "key_cols":    ["ingested_from"],
        "config":      "player_associations",
        "description": "Seasons in player associations",
    },
    "sandbox.ashish.nrl_player_associations_team": {
        "key_cols":    ["ingested_from"],
        "config":      "player_associations",
        "description": "Teams in player associations",
    },

    # ── matchdata ─────────────────────────────────────────────────────────────
    "sandbox.ashish.nrl_matchdata_gameinfo": {
        "key_cols":    ["game_id"],
        "config":      "matchdata",
        "description": "Match game info",
    },
    "sandbox.ashish.nrl_matchdata_teams": {
        "key_cols":    ["game_id", "ingested_from"],
        "config":      "matchdata",
        "description": "Match teams root",
    },
    "sandbox.ashish.nrl_matchdata_teamsmatch": {
        "key_cols":    ["game_id", "team_id"],
        "config":      "matchdata",
        "description": "Match team-level stats",
    },
    "sandbox.ashish.nrl_matchdata_teamcoach": {
        "key_cols":    ["game_id", "team_id"],
        "config":      "matchdata",
        "description": "Match team coaches",
    },
    "sandbox.ashish.nrl_matchdata_region": {
        "key_cols":    ["game_id", "ingested_from"],
        "config":      "matchdata",
        "description": "Match broadcast regions",
    },
    "sandbox.ashish.nrl_matchdata_broadcaster": {
        "key_cols":    ["game_id", "ingested_from"],
        "config":      "matchdata",
        "description": "Match broadcasters",
    },
    "sandbox.ashish.nrl_matchdata_insouts": {
        "key_cols":    ["game_id", "ingested_from"],
        "config":      "matchdata",
        "description": "Match ins/outs",
    },

    # ── matchstatsandevents ───────────────────────────────────────────────────
    "sandbox.ashish.nrl_matchstatsandevents_gamestats": {
        "key_cols":    ["game_id", "ingested_from"],
        "config":      "matchstatsandevents",
        "description": "Match stats root",
    },
    "sandbox.ashish.nrl_matchstatsandevents_eventflow": {
        "key_cols":    ["game_id", "ingested_from"],
        "config":      "matchstatsandevents",
        "description": "Match event flow root",
    },

    # ── livexy ────────────────────────────────────────────────────────────────
    "sandbox.ashish.nrl_livexy_teams": {
        "key_cols":    ["game_id", "ingested_from"],
        "config":      "livexy",
        "description": "Live XY teams root",
    },
    "sandbox.ashish.nrl_livexy_team": {
        "key_cols":    ["game_id", "team_id"],
        "config":      "livexy",
        "description": "Live XY per-team data",
    },

    # ── all_squads ────────────────────────────────────────────────────────────
    "sandbox.ashish.nrl_all_squads_competitions": {
        "key_cols":    ["season_id", "ingested_from"],
        "config":      "all_squads",
        "description": "All-squads root",
    },
    "sandbox.ashish.nrl_all_squads_competition": {
        "key_cols":    ["competition_id"],
        "config":      "all_squads",
        "description": "All-squads competition",
    },
    "sandbox.ashish.nrl_all_squads_team": {
        "key_cols":    ["team_id"],
        "config":      "all_squads",
        "description": "All-squads teams",
    },
    "sandbox.ashish.nrl_all_squads_player": {
        "key_cols":    ["player_id"],
        "config":      "all_squads",
        "description": "All-squads players",
    },
    "sandbox.ashish.nrl_all_squads_coach": {
        "key_cols":    ["coach_id"],
        "config":      "all_squads",
        "description": "All-squads coaches",
    },

    # ── squad_list ────────────────────────────────────────────────────────────
    "sandbox.ashish.nrl_squad_squads": {
        "key_cols":    ["competition_id", "season_id"],
        "config":      "squad_list",
        "description": "Squad list root",
    },
    "sandbox.ashish.nrl_squad_team": {
        "key_cols":    ["team_id"],
        "config":      "squad_list",
        "description": "Squad teams",
    },
    "sandbox.ashish.nrl_squad_player": {
        "key_cols":    ["player_id"],
        "config":      "squad_list",
        "description": "Squad players",
    },
    "sandbox.ashish.nrl_squad_coach": {
        "key_cols":    ["coach_id"],
        "config":      "squad_list",
        "description": "Squad coaches",
    },

    # ── entities ──────────────────────────────────────────────────────────────
    "sandbox.ashish.nrl_entities_competition": {
        "key_cols":    ["ingested_from"],
        "config":      "entities",
        "description": "Entity competitions",
    },
    "sandbox.ashish.nrl_entities_team_entity": {
        "key_cols":    ["ingested_from"],
        "config":      "entities",
        "description": "Entity teams",
    },
    "sandbox.ashish.nrl_entities_player_entity": {
        "key_cols":    ["ingested_from"],
        "config":      "entities",
        "description": "Entity players",
    },
    "sandbox.ashish.nrl_entities_venue_entity": {
        "key_cols":    ["ingested_from"],
        "config":      "entities",
        "description": "Entity venues",
    },
    "sandbox.ashish.nrl_entities_position": {
        "key_cols":    ["ingested_from"],
        "config":      "entities",
        "description": "Entity positions",
    },
    "sandbox.ashish.nrl_entities_statistic": {
        "key_cols":    ["ingested_from"],
        "config":      "entities",
        "description": "Entity statistics",
    },
}

print(f"Registry loaded — {len(SILVER_TABLES)} silver table(s) registered.")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# BRONZE TABLE REGISTRY
#
# Maps config name → bronze table + expected ingested_from prefix (LIKE pattern)
# Used by TC-B test group.
# ──────────────────────────────────────────────────────────────────────────────

BRONZE_TABLES = {
    "ladder":               {
        "table":   "sandbox.swathi.ladder",
        "pattern": f"ladder_{COMPETITION_ID}_{SEASON}",
    },
    "live_ladder":          {
        "table":   "sandbox.swathi.liveladder",
        "pattern": f"liveLadder_{COMPETITION_ID}_{SEASON}",
    },
    "player_profile":       {
        "table":   "sandbox.swathi.playerprofile",
        "pattern": f"playerProfiles_{COMPETITION_ID}_{SEASON}_%",
    },
    "player_associations":  {
        "table":   "sandbox.swathi.playerassociations",
        "pattern": None,   # non-parametric — no ingested_from filter
    },
    "matchdata":            {
        "table":   "sandbox.swathi.matchdata",
        "pattern": f"matchData_{COMPETITION_ID}_{SEASON}%",
    },
    "matchstatsandevents":  {
        "table":   "sandbox.swathi.matchstatsandevents",
        "pattern": f"matchStatsAndEvents_{COMPETITION_ID}_{SEASON}%",
    },
    "livexy":               {
        "table":   "sandbox.swathi.livexy",
        "pattern": f"liveXY_{COMPETITION_ID}_{SEASON}%",
    },
    "all_squads":           {
        "table":   "sandbox.swathi.allsquads",
        "pattern": f"allSquads_{COMPETITION_ID}_{SEASON}",
    },
    "squad_list":           {
        "table":   "sandbox.swathi.squadlist",
        "pattern": f"squad_{COMPETITION_ID}_{SEASON}%",
    },
    "entities":             {
        "table":   "sandbox.swathi.entities",
        "pattern": None,
    },
    "fixtures":             {
        "table":   "sandbox.swathi.fixtures",
        "pattern": f"fixtures_{COMPETITION_ID}_{SEASON}%",
    },
    "roundschedule":        {
        "table":   "sandbox.swathi.roundschedule",
        "pattern": f"roundSchedule_{COMPETITION_ID}_{SEASON}%",
    },
    "season_list":          {
        "table":   "sandbox.swathi.season",
        "pattern": f"seasonList_{COMPETITION_ID}",
    },
}

print(f"Bronze registry loaded — {len(BRONZE_TABLES)} config(s) registered.")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# TEST HARNESS
#
# All test results are accumulated in `_results` and printed as a final summary.
# ──────────────────────────────────────────────────────────────────────────────

_results = []   # list of dicts: {tc_id, description, status, detail}


def _check(tc_id, description, sql, expect_zero=True):
    """
    Execute `sql` — expected to return a single row with a column named `cnt`.
    PASS when cnt == 0  and  expect_zero=True   (e.g. "no bad rows found")
    PASS when cnt >  0  and  expect_zero=False  (e.g. "table has data")
    """
    try:
        row = spark.sql(sql).collect()[0]   # noqa: F821
        cnt = row["cnt"]
        if expect_zero:
            status = "PASS" if cnt == 0 else "FAIL"
            detail = f"bad rows = {cnt}" if cnt > 0 else "no bad rows"
        else:
            status = "PASS" if cnt > 0 else "FAIL"
            detail = f"row count = {cnt}"
    except Exception as e:
        status = "ERROR"
        detail = str(e)[:200]
        cnt    = None

    symbol = {"PASS": "✓", "FAIL": "✗", "WARN": "!", "ERROR": "⚠"}.get(status, "?")
    print(f"  [{symbol}] {tc_id:<12} {description[:60]:<60}  {detail}")
    _results.append({"tc_id": tc_id, "description": description,
                     "status": status, "detail": detail})


def _check_value(tc_id, description, sql, column, expected):
    """
    Execute `sql` — PASS when the value in `column` equals `expected`.
    """
    try:
        row    = spark.sql(sql).collect()[0]   # noqa: F821
        actual = row[column]
        status = "PASS" if actual == expected else "FAIL"
        detail = f"expected={expected}  actual={actual}"
    except Exception as e:
        status = "ERROR"
        detail = str(e)[:200]

    symbol = {"PASS": "✓", "FAIL": "✗", "WARN": "!", "ERROR": "⚠"}.get(status, "?")
    print(f"  [{symbol}] {tc_id:<12} {description[:60]:<60}  {detail}")
    _results.append({"tc_id": tc_id, "description": description,
                     "status": status, "detail": detail})


print("Test harness ready.")

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# TC-B  BRONZE LAYER HEALTH
#
# Verify that every bronze table is:
#   TC-B01  Readable   — a SELECT succeeds (table exists, permissions OK)
#   TC-B02  Non-empty  — at least one row exists for this competition/season
#   TC-B03  No null ingested_at — data quality baseline for watermarking
# ══════════════════════════════════════════════════════════════════════════════

print("\n── TC-B  Bronze layer health ──────────────────────────────────────────────")

for _cfg, _b in BRONZE_TABLES.items():
    _tbl      = _b["table"]
    _pattern  = _b["pattern"]
    _where    = f"WHERE ingested_from LIKE '{_pattern}'" if _pattern else ""
    _tc_base  = _cfg.upper()[:8]

    # TC-B01: table is reachable
    _check(
        f"TC-B01-{_tc_base}",
        f"[{_cfg}] Bronze table readable",
        f"SELECT COUNT(*) AS cnt FROM {_tbl}",
        expect_zero=False,
    )

    # TC-B02: rows exist for this competition/season
    if _pattern:
        _check(
            f"TC-B02-{_tc_base}",
            f"[{_cfg}] Bronze has rows for comp={COMPETITION_ID} season={SEASON}",
            f"SELECT COUNT(*) AS cnt FROM {_tbl} {_where}",
            expect_zero=False,
        )

    # TC-B03: no null ingested_at
    _check(
        f"TC-B03-{_tc_base}",
        f"[{_cfg}] Bronze ingested_at never NULL",
        f"SELECT COUNT(*) AS cnt FROM {_tbl} WHERE ingested_at IS NULL",
        expect_zero=True,
    )

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# TC-P  PIPELINE EXECUTION
#
# Validate the pipeline's own bookkeeping tables.
#
#   TC-P01  Audit log has an entry for each expected config (comp/season combo)
#   TC-P02  No errors in the error log for the most recent run per config
#   TC-P03  Audit log src_watermark is always > tgt_watermark (no backward step)
#   TC-P04  Audit log ingested_at is within the last 48 hours (stale run alert)
# ══════════════════════════════════════════════════════════════════════════════

print("\n── TC-P  Pipeline execution ───────────────────────────────────────────────")

# Configs that are parametric for this competition/season
_PARAMETRIC_CONFIGS = [
    f"ladder_{COMPETITION_ID}_{SEASON}",
    f"live_ladder_{COMPETITION_ID}_{SEASON}",
    f"player_profile_{COMPETITION_ID}_{SEASON}",
    f"matchdata_{COMPETITION_ID}_{SEASON}",
    f"matchstatsandevents_{COMPETITION_ID}_{SEASON}",
    f"livexy_{COMPETITION_ID}_{SEASON}",
    f"all_squads_{COMPETITION_ID}_{SEASON}",
    f"squad_list_{COMPETITION_ID}_{SEASON}",
    f"fixtures_{COMPETITION_ID}_{SEASON}",
    f"roundschedule_{COMPETITION_ID}_{SEASON}",
    f"season_list_{COMPETITION_ID}",
]
_NON_PARAMETRIC_CONFIGS = ["entities", "player_associations"]
_ALL_EXPECTED_KEYS = _PARAMETRIC_CONFIGS + _NON_PARAMETRIC_CONFIGS

# TC-P01: audit log entry exists for each expected config
for _key in _ALL_EXPECTED_KEYS:
    _check(
        "TC-P01",
        f"Audit entry exists: {_key}",
        f"""
        SELECT COUNT(*) AS cnt
        FROM   {AUDIT_LOG_TABLE}
        WHERE  config_name = '{_key}'
        """,
        expect_zero=False,
    )

# TC-P02: no ERROR-status rows in error log for any of the above configs
for _key in _ALL_EXPECTED_KEYS:
    _check(
        "TC-P02",
        f"No errors logged: {_key}",
        f"""
        SELECT COUNT(*) AS cnt
        FROM   {ERROR_LOG_TABLE}
        WHERE  config_name = '{_key}'
        """,
        expect_zero=True,
    )

# TC-P03: audit watermark never stepped backward
#         (src_watermark > tgt_watermark for the latest run of each config)
_check(
    "TC-P03",
    "Audit watermark always advances (no backward step)",
    f"""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT config_name,
               MAX(ingested_at)                  AS latest_run,
               MAX(src_watermark)                AS src_wm,
               MAX(tgt_watermark)                AS tgt_wm
        FROM   {AUDIT_LOG_TABLE}
        GROUP BY config_name
    )
    WHERE tgt_wm IS NOT NULL
      AND src_wm <= tgt_wm
    """,
    expect_zero=True,
)

# TC-P04: most recent audit entry is no older than 48 hours
_check(
    "TC-P04",
    "Latest audit entry is within the past 48 hours (freshness check)",
    f"""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT MAX(ingested_at) AS latest
        FROM   {AUDIT_LOG_TABLE}
    )
    WHERE latest < NOW() - INTERVAL 48 HOURS
    """,
    expect_zero=True,
)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# TC-S  SILVER TABLE ROW COUNTS
#
# Every silver table in the registry must contain at least one row.
# A zero-row table almost always means the pipeline ran but the source filter
# matched nothing — e.g. wrong ingested_from prefix, wrong competition/season.
# ══════════════════════════════════════════════════════════════════════════════

print("\n── TC-S  Silver table row counts ──────────────────────────────────────────")

for _tbl, _meta in SILVER_TABLES.items():
    _check(
        "TC-S01",
        f"[{_meta['config']}] {_meta['description']} has rows",
        f"SELECT COUNT(*) AS cnt FROM {_tbl}",
        expect_zero=False,
    )

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# TC-D  DEDUPLICATION INTEGRITY
#
# For each silver table, group by the dedup key columns and assert that no
# combination appears more than once.  Duplicate rows indicate that either:
#   • The dedup_keys_map in the config uses the wrong columns, or
#   • The pipeline ran multiple times without incrementally deduplicating.
# ══════════════════════════════════════════════════════════════════════════════

print("\n── TC-D  Deduplication integrity ──────────────────────────────────────────")

for _tbl, _meta in SILVER_TABLES.items():
    _key_cols_sql = ", ".join(_meta["key_cols"])
    _check(
        "TC-D01",
        f"[{_meta['config']}] {_meta['description']} — no duplicate dedup keys",
        f"""
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT {_key_cols_sql},
                   COUNT(*) AS n
            FROM   {_tbl}
            GROUP BY {_key_cols_sql}
            HAVING n > 1
        )
        """,
        expect_zero=True,
    )

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# TC-N  NULL KEY COLUMN CHECK
#
# Primary / dedup key columns must never be NULL.  A NULL key means a row
# cannot be identified or deduplicated correctly.  This typically happens when:
#   • The source JSON contained a missing field that maps to the key column, or
#   • A type override or rename caused the column to be silently dropped.
# ══════════════════════════════════════════════════════════════════════════════

print("\n── TC-N  Null key column check ────────────────────────────────────────────")

for _tbl, _meta in SILVER_TABLES.items():
    for _col in _meta["key_cols"]:
        _check(
            "TC-N01",
            f"[{_meta['config']}] {_meta['description']}.{_col} never NULL",
            f"""
            SELECT COUNT(*) AS cnt
            FROM   {_tbl}
            WHERE  `{_col}` IS NULL
            """,
            expect_zero=True,
        )

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# TC-Q  CONFIG-SPECIFIC DATA QUALITY
#
# Business-rule checks that are specific to individual configs.
# These go beyond generic structure checks to validate the actual content.
# ══════════════════════════════════════════════════════════════════════════════

print("\n── TC-Q  Config-specific data quality ─────────────────────────────────────")

# ── player_profile ────────────────────────────────────────────────────────────

# TC-Q01: every player has at least one team entry
#         (no player in the root table is missing from the teams child table)
_check(
    "TC-Q01",
    "[player_profile] All players have ≥1 team association",
    """
    SELECT COUNT(*) AS cnt
    FROM   sandbox.ashish.nrl_playerprofile_player p
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   sandbox.ashish.nrl_playerprofile_team t
        WHERE  t.player_id = p.player_id
    )
    """,
    expect_zero=True,
)

# TC-Q02: team table has both home and away teams per game
#         (each game_id should have exactly 2 rows — one home, one away)
_check(
    "TC-Q02",
    "[matchdata] Each game has exactly 2 team rows (home + away)",
    """
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT game_id, COUNT(*) AS n
        FROM   sandbox.ashish.nrl_matchdata_teamsmatch
        GROUP BY game_id
        HAVING n <> 2
    )
    """,
    expect_zero=True,
)

# TC-Q03: matchstatsandevents game stats have team-level stat rows
_check(
    "TC-Q03",
    "[matchstatsandevents] gamestats table is non-empty",
    "SELECT COUNT(*) AS cnt FROM sandbox.ashish.nrl_matchstatsandevents_gamestats",
    expect_zero=False,
)

# TC-Q04: matchstatsandevents event flow has rows
_check(
    "TC-Q04",
    "[matchstatsandevents] eventflow table is non-empty",
    "SELECT COUNT(*) AS cnt FROM sandbox.ashish.nrl_matchstatsandevents_eventflow",
    expect_zero=False,
)

# TC-Q05: livexy — each game should have exactly 2 team rows
_check(
    "TC-Q05",
    "[livexy] Each game has exactly 2 team rows",
    """
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT game_id, COUNT(*) AS n
        FROM   sandbox.ashish.nrl_livexy_team
        GROUP BY game_id
        HAVING n <> 2
    )
    """,
    expect_zero=True,
)

# TC-Q06: all_squads — every competition has at least one team
_check(
    "TC-Q06",
    "[all_squads] Every competition has at least one team",
    """
    SELECT COUNT(*) AS cnt
    FROM   sandbox.ashish.nrl_all_squads_competition c
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   sandbox.ashish.nrl_all_squads_team t
        WHERE  t.competition_id = c.competition_id
    )
    """,
    expect_zero=True,
)

# TC-Q07: all_squads — every team has at least one player
_check(
    "TC-Q07",
    "[all_squads] Every team has at least one player",
    """
    SELECT COUNT(*) AS cnt
    FROM   sandbox.ashish.nrl_all_squads_team t
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   sandbox.ashish.nrl_all_squads_player p
        WHERE  p.team_id = t.team_id
    )
    """,
    expect_zero=True,
)

# TC-Q08: squad_list — every team has at least one player
_check(
    "TC-Q08",
    "[squad_list] Every team has at least one player",
    """
    SELECT COUNT(*) AS cnt
    FROM   sandbox.ashish.nrl_squad_team t
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   sandbox.ashish.nrl_squad_player p
        WHERE  p.team_id = t.team_id
    )
    """,
    expect_zero=True,
)

# TC-Q09: entities — all core entity types are populated (quick sanity check)
for _entity_tbl, _label in [
    ("sandbox.ashish.nrl_entities_competition",  "competition"),
    ("sandbox.ashish.nrl_entities_team_entity",  "team"),
    ("sandbox.ashish.nrl_entities_player_entity","player"),
    ("sandbox.ashish.nrl_entities_position",     "position"),
    ("sandbox.ashish.nrl_entities_statistic",    "statistic"),
]:
    _check(
        "TC-Q09",
        f"[entities] {_label} entity table is non-empty",
        f"SELECT COUNT(*) AS cnt FROM {_entity_tbl}",
        expect_zero=False,
    )

# TC-Q10: matchdata — game_id in teamsmatch matches game_id in gameinfo
#         (no orphaned team-stat rows pointing to a non-existent game)
_check(
    "TC-Q10",
    "[matchdata] teamsMatch game_ids all exist in gameinfo",
    """
    SELECT COUNT(*) AS cnt
    FROM   sandbox.ashish.nrl_matchdata_teamsmatch t
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   sandbox.ashish.nrl_matchdata_gameinfo g
        WHERE  g.game_id = t.game_id
    )
    """,
    expect_zero=True,
)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# TC-I  INCREMENTAL INTEGRITY
#
# Validate watermark bookkeeping so that incremental runs don't re-process
# or skip data.
#
#   TC-I01  No silver rows have ingested_at earlier than the tgt_watermark
#           of the previous run (would mean old data was re-written on append)
#   TC-I02  Audit log has no duplicate config_name + ingested_at (each run
#           should produce exactly one audit entry per config)
# ══════════════════════════════════════════════════════════════════════════════

print("\n── TC-I  Incremental integrity ────────────────────────────────────────────")

# TC-I01: audit log has no duplicate (config_name, ingested_at) pairs
_check(
    "TC-I01",
    "Audit log: no duplicate (config_name, ingested_at) rows",
    f"""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT config_name, ingested_at, COUNT(*) AS n
        FROM   {AUDIT_LOG_TABLE}
        GROUP BY config_name, ingested_at
        HAVING n > 1
    )
    """,
    expect_zero=True,
)

# TC-I02: audit log src_watermark is never NULL for completed runs
_check(
    "TC-I02",
    "Audit log: src_watermark is always populated",
    f"""
    SELECT COUNT(*) AS cnt
    FROM   {AUDIT_LOG_TABLE}
    WHERE  src_watermark IS NULL
    """,
    expect_zero=True,
)

# TC-I03: error log has no rows where the error was not subsequently fixed
#         (i.e. if a config_name appears in error log it must also appear in
#          audit log with a LATER ingested_at — meaning it succeeded on retry)
_check(
    "TC-I03",
    "All error-log configs have a subsequent successful audit entry",
    f"""
    SELECT COUNT(*) AS cnt
    FROM (
        SELECT e.config_name,
               MAX(e.ingested_at) AS last_error,
               MAX(a.ingested_at) AS last_success
        FROM       {ERROR_LOG_TABLE}  e
        LEFT JOIN  {AUDIT_LOG_TABLE}  a
               ON  a.config_name = e.config_name
        GROUP BY e.config_name
    )
    WHERE last_success IS NULL
       OR last_success < last_error
    """,
    expect_zero=True,
)

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
#
# Print a formatted pass/fail table and a final counts line.
# The cell raises an exception if any test failed, so a Databricks job step
# running this notebook will show as FAILED when tests fail.
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "═" * 72)
print("  TEST SUMMARY")
print("═" * 72)
print(f"  {'TC ID':<14} {'Status':<8} {'Description':<45} {'Detail'}")
print("  " + "─" * 68)

_counts = {"PASS": 0, "FAIL": 0, "ERROR": 0, "WARN": 0}

for _r in _results:
    _sym = {"PASS": "✓", "FAIL": "✗", "ERROR": "⚠", "WARN": "!"}.get(_r["status"], "?")
    print(
        f"  [{_sym}] {_r['tc_id']:<12} {_r['status']:<8} "
        f"{_r['description'][:44]:<45} {_r['detail'][:40]}"
    )
    _counts[_r["status"]] = _counts.get(_r["status"], 0) + 1

print("═" * 72)
print(
    f"  TOTAL {len(_results)}  │  "
    f"PASS {_counts['PASS']}  │  "
    f"FAIL {_counts['FAIL']}  │  "
    f"ERROR {_counts['ERROR']}  │  "
    f"WARN {_counts['WARN']}"
)
print("═" * 72)

# Fail the notebook (and therefore the Databricks job step) if any test failed
if _counts["FAIL"] > 0 or _counts["ERROR"] > 0:
    raise AssertionError(
        f"{_counts['FAIL']} test(s) FAILED and {_counts['ERROR']} ERROR(s) — "
        f"see summary above."
    )
