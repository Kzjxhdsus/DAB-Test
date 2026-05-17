"""
NRL Entities API — Flattening Config
Source : api/NRL/configuration/entities
Bronze : one row per entity type (14 l1elements), all flat — no nested arrays.

Output tables (nrl_datalakehouse_prd.bronze_stats_flatten_<alias>):
  nrl_ground_condition   nrl_fantasy_rule     nrl_competition
  nrl_player_entity      nrl_position         nrl_round_entity
  nrl_scoring_method     nrl_season_entity    nrl_statistic
  nrl_team_entity        nrl_venue_entity     nrl_weather_condition
  nrl_game_state         nrl_official_entity

NOTE: verify bronze_table name with the data engineering team.
"""

ENTITIES_CONFIG = {
    "bronze_table":    "nrl_datalakehouse_prd.bronze_stats.entities",
    "target_prefix":   "nrl_datalakehouse_prd.bronze_stats_flatten.entities",
    "exclude_fields": set(),

    "exclude_root_aliases": {
        "ground_condition",  "fantasy_rule",
        "player_entity",      "round_entity",
        "scoring_method",    "season_entity",
        "team_entity",       "venue_entity",   "weather_condition",
        "game_state",        "official_entity",
        # Distinct root aliases for the 3 same-name cases:
        "competitions_root", "positions_root",  "statistics_root",
    },
    "flatten_structs":    {},
    "force_array_fields": set(),

    "elements": {
        "groundConditions":  "ground_condition",
        "fantasyRules":      "fantasy_rule",
        "competitions":      "competitions_root",
        "players":           "player_entity",
        "positions":         "positions_root",
        "rounds":            "round_entity",
        "scoringMethods":    "scoring_method",
        "seasons":           "season_entity",
        "statistics":        "statistics_root",
        "teams":             "team_entity",
        "venues":            "venue_entity",
        "weatherConditions": "weather_condition",
        "gameStates":        "game_state",
        "officials":         "official_entity",
    },

    "dedup_keys_map": {
        "ground_condition":   ["ingested_from"],
        "groundCondition":    ["id"],
        "fantasy_rule":       ["ingested_from"],
        "fantasyRule":        ["id", "competition_id", "season_id"],   # fantasyRules is competition-scoped
        "competitions_root":  ["ingested_from"],
        "competition":        ["id"],
        "player_entity":      ["ingested_from"],
        "player":             ["id"],
        "positions_root":     ["ingested_from"],
        "position":           ["id"],
        "round_entity":       ["ingested_from"],
        "round":              ["id", "competition_id", "season_id"],  # scoped to competition + season
        "scoring_method":     ["ingested_from"],
        "scoringMethod":      ["id"],
        "season_entity":      ["ingested_from"],
        "season":             ["id"],
        "statistics_root":    ["ingested_from"],
        "statistic":          ["id"],
        "team_entity":        ["ingested_from"],
        "team":               ["id"],
        "venue_entity":       ["ingested_from"],
        "venue":              ["id"],
        "weather_condition":  ["ingested_from"],
        "weatherCondition":   ["id"],
        "game_state":         ["ingested_from"],
        "gameState":          ["id"],
        "official_entity":    ["ingested_from"],
        "official":           ["id"],
    },

    "sample_size":    200,
    "type_overrides": {},
}