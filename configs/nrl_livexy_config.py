"""
NRL LiveXY API — Flattening Config
Source : nrl_datalakehouse_prd.bronze_stats.live_xy
Bronze : two l1elements —
           "videos"  → empty payload, skipped entirely
           "teams"   → L1 mode (no l0payload), one row per team per game.
                       Each team payload is ~5.5 MB containing two XY stat arrays
                       (xandYFeed1stHalf.stat, xandYFeed2ndHalf.stat) with ~5,000
                       coordinate entries each.  These are excluded from the main
                       pipeline and handled by the optimised post_run_fn below.

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.live_xy):
  nrl_livexy_teams      ← one row per team per game (scalar team fields only)
  nrl_livexy_teams_stat ← created by post_run_fn: one row per XY stat event,
                           both halves unified with a feed_period column
                           (1 = 1st half, 2 = 2nd half)

Performance note
----------------
xandYFeed1stHalf / xandYFeed2ndHalf each contain ~5,000 entries per row.
sample_size is set to 2 so schema inference does not scan 200 × 5.5 MB payloads.
The post_run_fn uses a typed StructType schema (not MAP) and repartitions before
exploding to avoid OOM on large JSON rows.
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------------------------
# Typed schema for a single XY stat entry
# All values arrive as strings in the JSON
# ---------------------------------------------------------------------------
_STAT_SCHEMA = T.ArrayType(
    T.StructType([
        T.StructField("AS",  T.StringType(), True),
        T.StructField("EX",  T.StringType(), True),
        T.StructField("EY",  T.StringType(), True),
        T.StructField("GM",  T.StringType(), True),
        T.StructField("GS",  T.StringType(), True),
        T.StructField("HS",  T.StringType(), True),
        T.StructField("PI",  T.StringType(), True),
        T.StructField("PN",  T.StringType(), True),
        T.StructField("SD",  T.StringType(), True),
        T.StructField("SI",  T.StringType(), True),
        T.StructField("SN",  T.StringType(), True),
        T.StructField("SV",  T.StringType(), True),
        T.StructField("T",   T.StringType(), True),
        T.StructField("TN",  T.StringType(), True),
        T.StructField("UTC", T.StringType(), True),
        T.StructField("VR",  T.StringType(), True),
        T.StructField("ZO",  T.StringType(), True),
    ])
)


# ---------------------------------------------------------------------------
# post_run_fn: unify xandYFeed1stHalf / xandYFeed2ndHalf stat arrays
# ---------------------------------------------------------------------------
def _create_livexy_stat_table(config, spark):
    """
    Reads the bronze livexy table and explodes each xandYFeedNHalf.stat array
    into a single Delta table with a feed_period discriminator column.

    Uses a typed StructType schema (not MAP) for fast from_json parsing, and
    repartitions before explode to prevent Spark task skew on large JSON rows.
    """
    bronze        = config["bronze_table"]
    target_prefix = config["target_prefix"]
    stat_table    = f"{target_prefix}_teams_stat"

    _bronze_df = spark.table(bronze)  # noqa: F821
    _bf = config.get("bronze_filter")
    if _bf:
        _bronze_df = _bronze_df.filter(_bf)

    df = _bronze_df.filter(F.col("l1element") == "teams")

    if df.limit(1).count() == 0:
        print(f"[livexy post_run_fn] No 'teams' rows in {bronze} — skipping {stat_table}")
        return

    # ── Extract team scalars + raw stat JSON strings ──────────────────────
    # Repartition to 200 partitions so each task handles a small slice of
    # the large payload strings instead of one huge task per row.
    base = (
        df.repartition(200)
        .select(
            F.col("ingested_from"),
            F.col("ingested_at"),
            F.get_json_object(F.col("payload").cast("string"), "$.team[0].teamId")
             .alias("team_id"),
            F.get_json_object(F.col("payload").cast("string"), "$.team[0].teamName")
             .alias("team_name"),
            F.get_json_object(F.col("payload").cast("string"), "$.team[0].isHomeTeam")
             .alias("is_home_team"),
            # Extract raw stat arrays as JSON strings for typed parsing
            F.get_json_object(F.col("payload").cast("string"), "$.team[0].xandYFeed1stHalf.stat")
             .alias("stat_json_1"),
            F.get_json_object(F.col("payload").cast("string"), "$.team[0].xandYFeed2ndHalf.stat")
             .alias("stat_json_2"),
        )
    )

    frames = []
    for period_num, stat_col in [(1, "stat_json_1"), (2, "stat_json_2")]:
        exploded = (
            base
            .select(
                "ingested_from",
                "ingested_at",
                "team_id",
                "team_name",
                "is_home_team",
                F.lit(period_num).cast(T.IntegerType()).alias("feed_period"),
                F.explode_outer(
                    F.from_json(F.col(stat_col), _STAT_SCHEMA)
                ).alias("s"),
            )
            .select(
                "ingested_from",
                "ingested_at",
                "team_id",
                "team_name",
                "is_home_team",
                "feed_period",
                F.col("s.SI").alias("si"),
                F.col("s.SN").alias("sn"),
                F.col("s.SV").alias("sv"),
                F.col("s.T").alias("t"),
                F.col("s.GM").alias("gm"),
                F.col("s.PI").alias("pi"),
                F.col("s.PN").alias("pn"),
                F.col("s.EX").alias("ex"),
                F.col("s.EY").alias("ey"),
                F.col("s.ZO").alias("zo"),
                F.col("s.AS").alias("as_stat"),   # AS is a reserved word
                F.col("s.GS").alias("gs"),
                F.col("s.HS").alias("hs"),
                F.col("s.VR").alias("vr"),
                F.col("s.TN").alias("tn"),
                F.col("s.SD").alias("sd"),
                F.col("s.UTC").alias("utc"),
            )
            .filter(F.col("si").isNotNull())
        )
        frames.append(exploded)

    combined = frames[0].unionByName(frames[1], allowMissingColumns=True)

    # Deduplicate by natural key: one stat entry is unique per
    # (team, game ingestion, period, stat-id)
    combined = combined.dropDuplicates(["team_id", "ingested_from", "feed_period", "si"])

    write_mode = config.get("write_mode", "overwrite")
    row_count = combined.count()
    (
        combined.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .saveAsTable(stat_table)
    )
    print(f"[livexy post_run_fn] Written {row_count} rows to {stat_table} (mode={write_mode})")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LIVEXY_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.live_xy",      # ← verify
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.live_xy",

    # Prevent the framework from selecting or schema-scanning these huge arrays.
    "source_filter_template": "ingested_from LIKE 'live_xy_{competition_id}_{season}%'",

    # Exclude the huge XY stat arrays — post_run_fn handles them directly from
    # bronze.  "stat" is also excluded so the framework doesn't try to process
    # the stat array that appears as a child of xandYFeed{1st,2nd}Half.
    "exclude_fields": {"xandYFeed1stHalf", "xandYFeed2ndHalf", "stat"},

    "flatten_structs":    {},
    "force_array_fields": set(),

    # Only process "teams" rows; "videos" rows have an empty payload.
    "elements": {
        "teams": "teams",
    },

    "dedup_keys_map": {
        # Root "teams" table: one row per game — L0 scalars only, no team_id here
        "teams": ["game_id", "ingested_from"],
        # Child "team" table: one row per team per game
        "team":  ["game_id", "team_id"],
    },

    # Use a tiny sample so schema inference doesn't parse 200 × 5.5 MB payloads.
    "sample_size":    2,
    "type_overrides": {},

    "post_run_fn": _create_livexy_stat_table,
}
