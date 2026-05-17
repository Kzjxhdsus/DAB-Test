"""
NRL Player Associations API — Flattening Config
Source : api/NRL/playerAssociations
Bronze : one l1element (players), single large payload containing every
         player's full competition/season/team history.

Payload structure (JSON after XML conversion)
---------------------------------------------
payload = [
  {
    "playerId": "1", "playerFirstName": "Aaron", ...
    "competitions": {
      "competition": {           <- dict when player has 1 competition (XML single-element)
        "competitionId": "111",    force_array_fields: {"competition"} handles this
        "competitionName": "NRL Telstra Premiership",
        "seasons": {
          "season": [            <- list when player has multiple seasons
            {                    <- dict when player has 1 season (XML single-element)
              "seasonId": "2002",  force_array_fields: {"season"} handles this
              "teams": {
                "team": {        <- dict when player had 1 team that season (XML single-element)
                  "teamId": "6",   force_array_fields: {"team"} handles this
                  "teamName": "Northern Eagles"
                  "positions": {} <- empty struct, excluded via exclude_fields
                }
              }
            }, ...
          ]
        }
      }
    }
  }, ...
]

Output tables (target_prefix = nrl_datalakehouse_prd.bronze_stats_flatten.player_associations):
  nrl_player_associations_players      <- one row per player (root, exploded from payload array)
  nrl_player_associations_competition  <- one row per player x competition
  nrl_player_associations_season       <- one row per player x competition x season
  nrl_player_associations_team         <- one row per player x competition x season x team
"""

PLAYER_ASSOCIATIONS_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.player_associations",      # ← verify
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.player_associations",

    # positions is an empty struct {} on every team — no useful data, exclude it.
    # players (root wrapper) contains no scalar fields — only the child player array.
    # Excluding it suppresses the thin nrl_player_associations_players table;
    # all real data starts at nrl_player_associations_player.
    # positions is an empty struct {} on every team — no useful data, exclude it.
    "exclude_fields":  {"positions"},

# players (root wrapper) contains no scalar fields — only the child player array.
# exclude_root_aliases suppresses the thin nrl_player_associations_players table
# while still allowing the framework to process and write the player child table.
    "exclude_root_aliases": {"players"},

    "flatten_structs": {},

    # competition: XML single-element -> dict instead of list.
    # season:      XML single-element -> dict instead of list (players with 1 season).
    # team:        XML single-element -> dict instead of list (one team per season).
    # force_array_fields ensures all three always produce their own child tables.
    "force_array_fields": {"competition", "season", "team"},

    # Single l1element; alias "players" -> root table nrl_player_associations_players.
    # The payload array is exploded so each root row IS one player.
    "elements": {
        "players": "players",
    },

    "dedup_keys_map": {
        # Root "players" table: one row per ingestion — the payload OBJECT has no
        # scalar fields at this level (only the child "player" array), so only
        # ingested_from is available as a dedup key here.
        "players":     ["ingested_from"],
        # "player" child: one row per player after the payload array is exploded.
        "player":      ["player_id"],
        # Each level includes all ancestor keys so the full FK chain propagates down.
        "competition": ["player_id", "competition_id"],
        "season":      ["player_id", "competition_id", "season_id"],
        "team":        ["player_id", "competition_id", "season_id", "team_id"],
    },

    "sample_size":    200,
    "type_overrides": {},
}
