"""
NRL Player Profile API — Flattening Config
Source : api/NRL/playerProfiles/{competitionId}/{seasonId}/{playerId}

Bronze layout (L0 mode)
-----------------------
The ingestion pipeline stores this API's data with the root entity's scalar
attributes in l0payload (one row per player) and the player's child arrays
(teams) in the l1 payload column:

  l0payload : {"playerId":"500086","playerFirstName":"Adam","careerAppearances":"312",...}
  l1element : "player"
  payload   : [{"teamId":"500005","teamName":"South Sydney Rabbitohs",...}, ...]

The framework detects non-null l0payload automatically and switches to L0 merge
mode: l0payload scalars + payload child arrays are merged into one entity JSON
per row, with no root-level array split/explode needed.

Output tables  (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten):
  nrl_player  ← one row per player  (playerId + career stats from l0payload)
  nrl_team    ← one row per player × club  (club history from payload teams array)
               design doc calls this nrl_player_previous_clubs; rename if required

NOTE: verify bronze_table name with the data engineering team.
"""

PLAYER_PROFILE_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.player_profiles",      # ← verify
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.player_profiles",
    "exclude_fields":  set(),
    "flatten_structs": {},

    # l1element = "player" (one row per player in the bronze table).
    # l0payload holds the player's scalar attributes — detected automatically.
    # Root alias "player" → table nrl_player.
    "elements": {
        "player": "player",
    },

    # Auto-discovered nesting: player (from l0payload) → teams.team (from payload)
    "dedup_keys_map": {
        "player": ["player_id"],
        "team":   ["player_id", "team_id"],   # club history rows
    },

    # "team" uses the XML single-element pattern: one team → dict, multiple → list.
    # force_array_fields ensures it always gets its own child table regardless.
    "force_array_fields": {"team"},

    # Filter to a specific competition/season when those job params are supplied.
    # ingested_from format: playerProfiles_{competitionId}_{seasonId}_{playerId}
    # LIKE used because the playerId suffix varies per row — we match on the
    # competition/season prefix only.  ← verify prefix
    "source_filter_template": "ingested_from LIKE 'player_profiles_{competition_id}_{season}'",

    "sample_size":    200,
    "type_overrides": {},
}
