-- Databricks notebook source
-- Source schema : sandbox.swathi   (bronze_stats_flatten tables)
-- Target schema : sandbox.ashish   (silver dimension + fact tables)
--
-- Run cells in order 1 → 12.
-- FK constraints are INFORMATIONAL only — Unity Catalog does not enforce them.
-- SCD2 surrogate keys are populated by silver_02_dim_populate.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Silver Layer — Dimension DDL
-- MAGIC | | |
-- MAGIC |---|---|
-- MAGIC | **Source** | `sandbox.swathi` (bronze flatten) |
-- MAGIC | **Target** | `sandbox.ashish` (silver) |
-- MAGIC | **Build order** | Run cells top → bottom — each FK references a table from a prior cell |
-- MAGIC | **SCD type** | Type 1 for lookups; Type 2 for team / player / coach |
-- MAGIC | **Surrogate keys** | Populated by `silver_02_dim_populate`; DDL only defines structure |

-- COMMAND ----------

-- ============================================================
-- 1. Create target schema
-- ============================================================
CREATE SCHEMA IF NOT EXISTS sandbox.ashish
COMMENT 'Silver layer — cleansed, deduplicated, SCD2-versioned dimensions and facts';

-- COMMAND ----------

-- ============================================================
-- 2. dim_competition
-- Source : sandbox.swathi.entities_competition
-- SCD    : Type 1 — competition metadata rarely changes
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_competition (
    competition_id        STRING    NOT NULL  COMMENT 'Natural PK — from entities API (id field)',
    competition_name      STRING              COMMENT 'Full name e.g. "NRL Premiership"',
    short_name            STRING              COMMENT 'Abbreviation e.g. "NRL"',
    sport                 STRING              COMMENT 'Always "Rugby League" for this source',
    -- audit columns
    _source_table         STRING              COMMENT 'Bronze table this row came from',
    _loaded_at            TIMESTAMP           COMMENT 'When first loaded to silver',
    _updated_at           TIMESTAMP           COMMENT 'When last updated in silver',
    CONSTRAINT pk_dim_competition PRIMARY KEY (competition_id)
)
USING DELTA
COMMENT 'Competition master — one row per competition (NRL, NRLW, State of Origin, etc.)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 3. dim_season
-- Source : sandbox.swathi.entities_season  (primary)
--        + sandbox.swathi.season_list_season (active / current_season flags)
-- SCD    : Type 1
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_season (
    season_id             STRING    NOT NULL  COMMENT 'Natural PK — year string e.g. "2024"',
    competition_id        STRING    NOT NULL  COMMENT 'FK → dim_competition',
    season_name           STRING              COMMENT 'Display name e.g. "2024 NRL Season"',
    season_year           INT                 COMMENT 'Numeric year — useful for range filters',
    active                BOOLEAN             COMMENT 'Season currently active (from season_list)',
    current_season        BOOLEAN             COMMENT 'Is the current / latest season',
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_season             PRIMARY KEY (season_id),
    CONSTRAINT fk_dim_season_competition FOREIGN KEY (competition_id)
        REFERENCES sandbox.ashish.dim_competition (competition_id)
)
USING DELTA
COMMENT 'Season master — one row per season per competition'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 4. dim_round
-- Source : sandbox.swathi.season_list_round
-- SCD    : Type 1
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_round (
    round_id              STRING    NOT NULL  COMMENT 'Natural PK — from season_list API (id field)',
    season_id             STRING    NOT NULL  COMMENT 'FK → dim_season',
    competition_id        STRING    NOT NULL  COMMENT 'FK → dim_competition',
    round_name            STRING              COMMENT 'e.g. "Round 1", "Semi Final"',
    round_abbreviation    STRING              COMMENT 'e.g. "R1", "SF"',
    round_number          INT                 COMMENT 'Numeric sort order — parsed from round_name',
    round_type            STRING              COMMENT '"Regular" | "Finals" | "Qualifying" | "Elimination"',
    is_finals             BOOLEAN             COMMENT 'Derived: TRUE when round_type != Regular',
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_round        PRIMARY KEY (round_id),
    CONSTRAINT fk_dim_round_season FOREIGN KEY (season_id)
        REFERENCES sandbox.ashish.dim_season (season_id)
)
USING DELTA
COMMENT 'Round master — one row per round per season'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 5. dim_venue
-- Source : sandbox.swathi.entities_venue
-- SCD    : Type 1
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_venue (
    venue_id              STRING    NOT NULL  COMMENT 'Natural PK — from entities API',
    venue_name            STRING,
    city                  STRING,
    state                 STRING,
    country               STRING,
    capacity              INT,
    surface_type          STRING              COMMENT '"Grass" | "Synthetic"',
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_venue PRIMARY KEY (venue_id)
)
USING DELTA
COMMENT 'Venue / ground master'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 6. dim_position
-- Source : sandbox.swathi.entities_position
-- SCD    : Type 1 — small static lookup (17 positions)
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_position (
    position_id           STRING    NOT NULL  COMMENT 'Natural PK — from entities API',
    position_name         STRING              COMMENT 'e.g. "Fullback"',
    position_abbreviation STRING              COMMENT 'e.g. "FB"',
    position_group        STRING              COMMENT '"Back" | "Forward" | "Interchange"',
    jersey_number         INT                 COMMENT 'Typical jersey number for this position',
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_position PRIMARY KEY (position_id)
)
USING DELTA
COMMENT 'Playing position lookup — 17 starting positions + interchange'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 7. dim_scoring_method
-- Source : sandbox.swathi.entities_scoring_method
-- SCD    : Type 1 — small static lookup
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_scoring_method (
    method_id             STRING    NOT NULL  COMMENT 'Natural PK — from entities API',
    method_name           STRING              COMMENT '"Try" | "Conversion" | "Penalty Goal" | "Drop Goal"',
    points                INT                 COMMENT '4 | 2 | 2 | 1',
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_scoring_method PRIMARY KEY (method_id)
)
USING DELTA
COMMENT 'Scoring method lookup — points value per scoring type'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 8. dim_team  *** SCD TYPE 2 ***
-- Source : sandbox.swathi.entities_team   (primary — name, abbreviation)
--        + sandbox.swathi.season_list_club (nickname, colours)
-- New row when: team_name, team_nickname, or team_abbreviation changes
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_team (
    -- Surrogate key: sha2(concat_ws('|', team_id, cast(valid_from as string)), 256)
    team_sk               STRING    NOT NULL  COMMENT 'Surrogate PK — hash of team_id + valid_from',
    -- Natural key
    team_id               STRING    NOT NULL  COMMENT 'NRL API team identifier',
    -- FK context
    competition_id        STRING              COMMENT 'FK → dim_competition',
    season_id             STRING              COMMENT 'Season this version was first active',
    -- Tracked SCD2 attributes (change triggers new row)
    team_name             STRING              COMMENT 'Full club name e.g. "South Sydney Rabbitohs"',
    team_nickname         STRING              COMMENT 'Short name e.g. "Rabbitohs"',
    team_abbreviation     STRING              COMMENT '3-letter code e.g. "SSR"',
    team_colour_primary   STRING,
    team_colour_secondary STRING,
    -- SCD2 version controls
    valid_from            TIMESTAMP NOT NULL  COMMENT 'When this version became active',
    valid_to              TIMESTAMP           COMMENT 'NULL = current version',
    is_current            BOOLEAN   NOT NULL  COMMENT 'TRUE for the active version',
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_team             PRIMARY KEY (team_sk),
    CONSTRAINT fk_dim_team_competition FOREIGN KEY (competition_id)
        REFERENCES sandbox.ashish.dim_competition (competition_id)
)
USING DELTA
COMMENT 'Team SCD2 — new row when name or abbreviation changes across seasons'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 9. dim_player  *** SCD TYPE 2 ***
-- Source : sandbox.swathi.player_profiles_player  (primary — bio, career stats)
--        + sandbox.swathi.entities_player          (fallback if profile missing)
-- New row when: position, weight, or height changes materially
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_player (
    -- Surrogate key: sha2(concat_ws('|', player_id, cast(valid_from as string)), 256)
    player_sk             STRING    NOT NULL  COMMENT 'Surrogate PK — hash of player_id + valid_from',
    -- Natural key
    player_id             STRING    NOT NULL  COMMENT 'NRL API player identifier',
    -- Attributes
    first_name            STRING,
    last_name             STRING,
    display_name          STRING              COMMENT 'Formatted full name for reporting',
    date_of_birth         DATE,
    height_cm             INT,
    weight_kg             INT,
    nationality           STRING,
    country_of_birth      STRING,
    -- Default position (game-level position is stored on fact_player_game)
    position_id           STRING              COMMENT 'FK → dim_position (primary/default position)',
    career_debut_year     INT,
    career_appearances    INT                 COMMENT 'Total career games from player_profiles API',
    -- SCD2 version controls
    valid_from            TIMESTAMP NOT NULL,
    valid_to              TIMESTAMP           COMMENT 'NULL = current version',
    is_current            BOOLEAN   NOT NULL,
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_player          PRIMARY KEY (player_sk),
    CONSTRAINT fk_dim_player_position FOREIGN KEY (position_id)
        REFERENCES sandbox.ashish.dim_position (position_id)
)
USING DELTA
COMMENT 'Player SCD2 — new row when position, weight, or other tracked attributes change'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 10. dim_coach  *** SCD TYPE 2 ***
-- Source : sandbox.swathi.season_list_coach  (primary)
--        + sandbox.swathi.all_squads_coach   (cross-competition coverage)
-- New row when: coach changes team or role
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_coach (
    -- Surrogate key: sha2(concat_ws('|', coach_id, cast(valid_from as string)), 256)
    coach_sk              STRING    NOT NULL  COMMENT 'Surrogate PK — hash of coach_id + valid_from',
    -- Natural key
    coach_id              STRING    NOT NULL  COMMENT 'NRL API coach identifier',
    -- FK context
    season_id             STRING    NOT NULL  COMMENT 'FK → dim_season',
    team_id               STRING              COMMENT 'Team coached in this version',
    -- Attributes
    first_name            STRING,
    last_name             STRING,
    display_name          STRING,
    coaching_role         STRING              COMMENT '"Head Coach" | "Assistant" | "Development"',
    -- SCD2 version controls
    valid_from            TIMESTAMP NOT NULL,
    valid_to              TIMESTAMP           COMMENT 'NULL = current version',
    is_current            BOOLEAN   NOT NULL,
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_coach        PRIMARY KEY (coach_sk),
    CONSTRAINT fk_dim_coach_season FOREIGN KEY (season_id)
        REFERENCES sandbox.ashish.dim_season (season_id)
)
USING DELTA
COMMENT 'Coach SCD2 — new row when coach changes team or role'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 11. dim_official
-- Source : sandbox.swathi.season_list_official
-- SCD    : Type 1 — officials registered per season, no mid-season changes
-- ============================================================
CREATE TABLE IF NOT EXISTS sandbox.ashish.dim_official (
    official_id           STRING    NOT NULL  COMMENT 'Natural PK — from season_list API',
    season_id             STRING    NOT NULL  COMMENT 'FK → dim_season (officials are season-scoped)',
    first_name            STRING,
    last_name             STRING,
    display_name          STRING,
    role                  STRING              COMMENT '"Referee" | "Touch Judge" | "Video Referee"',
    -- audit
    _source_table         STRING,
    _loaded_at            TIMESTAMP,
    _updated_at           TIMESTAMP,
    CONSTRAINT pk_dim_official        PRIMARY KEY (official_id, season_id),
    CONSTRAINT fk_dim_official_season FOREIGN KEY (season_id)
        REFERENCES sandbox.ashish.dim_season (season_id)
)
USING DELTA
COMMENT 'Match officials — registered per season'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- COMMAND ----------

-- ============================================================
-- 12. Verify
-- ============================================================
SHOW TABLES IN sandbox.ashish;
