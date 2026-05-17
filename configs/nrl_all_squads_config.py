"""
NRL All Squads API — Flattening Config
Source : api/NRL/competitions/allSquads/{competitionId}/{seasonId}
         (XML root: SquadsBySeason — verify exact endpoint with data engineering)
Bronze : one l1element (competitions), nested competition → teams with player
         & coach sub-arrays. teamHistory is a flat struct on the team —
         inlined as columns via flatten_structs rather than exploded into a
         separate table.

Output tables  (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten):
  nrl_all_squads_competitions  ← root  (one row per competition; competition_id is the dedup key)
  nrl_all_squads_team          ← Level 2  (one row per team, teamHistory inlined as team_history_* columns)
  nrl_all_squads_player        ← Level 3 sibling
  nrl_all_squads_coach         ← Level 3 sibling

NOTE: verify bronze_table name with the data engineering team.
"""

ALL_SQUADS_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.all_squads",      # ← verify
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.all_squads",
    "exclude_fields":  set(),

    # teamHistory is a single XML element → JSON struct with flat attributes.
    # flatten_structs inlines those attributes as team_history_* columns on
    # nrl_all_squads_team (same pattern as nrl_squad_list_config).
    "flatten_structs": {"teamHistory": "team_history_"},

    # coach appears as a single <coach .../> tag per team in most cases.
    # The XML-to-JSON converter turns a single child tag into a dict (not a list),
    # so _discover_arrays would never see it as an array.
    # force_array_fields tells the framework: "treat coach as an array regardless"
    # so it always gets its own nrl_all_squads_coach table.
    "force_array_fields": {"coach"},

    # Single l1element; alias "competitions" → root table nrl_all_squads_competitions.
    "elements": {
        "competitions": "competitions",
    },

    "dedup_keys_map": {
        # Root "competitions" table: L0 mode — l0payload only has seasonId, so
        # the root table has season_id + ingested_from/at.  competition_id lives
        # inside the child "competition" array, not here.
        "competitions": ["season_id", "ingested_from"],
        # "competition" child: one row per competition (exploded from payload array).
        # competition_id is propagated as FK to all descendant tables.
        "competition":  ["competition_id"],
        "team":         ["team_id"],                 # Level 3 (inherits competition_id)
        "player":       ["player_id"],               # Level 4
        "coach":        ["coach_id"],                # Level 4
    },

    # Filter to a specific competition/season when those job params are supplied.
    # ingested_from format: allSquads_{competitionId}_{seasonId}
    "source_filter_template": "ingested_from = 'all_squads_{competition_id}_{season}'",

    "sample_size":    200,
    "type_overrides": {},
}
