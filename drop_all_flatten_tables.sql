-- ============================================================
-- DROP ALL FLATTEN OUTPUT TABLES — 12 Medium-Complexity Configs
-- nrl_datalakehouse_qa.bronze_stats_flatten
--
-- Each config produces two sets of tables:
--   Framework tables — written automatically by run_multi_table_pipeline
--                      (one root table per l1element, no child arrays
--                       because all nested fields are in exclude_fields)
--   post_run_fn tables — written by the config's post_run_fn
--
-- Run this to fully reset before a clean re-run.
-- ============================================================


-- ============================================================
-- 1. competitionteamtopthreeleaderboardreport
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_top_three_leaderboard_teams;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_top_three_leaderboard;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_top_three_leaderboard_team;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_top_three_leaderboard_stat;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_top_three_leaderboard_entry;


-- ============================================================
-- 2. competitionteamcareerbestleaderboardreport
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_career_best_leaderboard_stats;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_career_best_leaderboard;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_career_best_leaderboard_stat;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_career_best_leaderboard_entry;


-- ============================================================
-- 3. playerstatsmaster  (historical_stats_by_round)
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_players;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_player;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_career_total;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_career_average;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_career_team_total;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_career_team_avg;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_season_total;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_season_average;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_season_team_total;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_season_team_avg;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_historical_stats_by_round_round;


-- ============================================================
-- 4. teamleaderboards
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_stats;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_leaderboard_stat;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_leaderboard_entry;


-- ============================================================
-- 5. teamleaderboard  (team_history_leaderboard)
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_history_leaderboard_stats;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_history_leaderboard;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_history_leaderboard_stat;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_history_leaderboard_entry;


-- ============================================================
-- 6. matchmilestones  (upcoming_match_milestones)
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_upcoming_match_milestones_teams;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_upcoming_match_milestones;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_upcoming_match_milestones_team_stat;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_upcoming_match_milestones_player_stat;


-- ============================================================
-- 7. formguideformatch
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_last_ten_meetings;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_team_form_guide_stats;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_last_ten_meeting;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_team;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_h2h;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_recent_game;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_season_home_away;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_player_game;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_season_team_stats;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_win_loss;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_form_guide_for_match_team_lineup;


-- ============================================================
-- 8. teamleaderboardsaverage
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_average_stats;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_average;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_average_leaderboard_stat;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_average_leaderboard_entry;


-- ============================================================
-- 9. teamleaderboardsperformance
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_performance_stats;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_performance;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_performance_leaderboard_stat;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_team_leaderboards_performance_leaderboard_entry;


-- ============================================================
-- 10. matchsnapshotdata
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_game_info;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_teams;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_interchange_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_injuries_and_suspensions;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_discipline_flow;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_team;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_team_player;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_interchange;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_injuries_suspensions;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_snapshot_discipline;


-- ============================================================
-- 11. eventflow  (match_events_flow)
-- ============================================================

-- Framework (one root table per l1element, all child arrays excluded)
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_score_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_possession_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_penalty_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_error_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_scrum_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_interchange_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_commentary_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_colour_events_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_discipline_flow;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_score_team;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_score_event;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_possession;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_penalty;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_error;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_scrum;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_interchange;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_commentary;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_colour;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_match_events_flow_discipline;


-- ============================================================
-- 12. playermilestones
-- ============================================================

-- Framework
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_player_milestones_stats;

-- post_run_fn
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_player_milestones;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_player_milestones_leaderboard_stat;
DROP TABLE IF EXISTS nrl_datalakehouse_qa.bronze_stats_flatten.nrl_player_milestones_milestone_entry;


-- ============================================================
-- Audit & Error Log Tables
-- NOTE: In Databricks Workflow mode these are set by Task 1
-- (Initialize_Log_Tables) and may differ from the fallback names
-- below. Update if your workflow uses a different catalog/schema.
-- ============================================================

DROP TABLE IF EXISTS sandbox.ashish.nrl_pipeline_audit_log;
DROP TABLE IF EXISTS sandbox.ashish.nrl_pipeline_error_log;
