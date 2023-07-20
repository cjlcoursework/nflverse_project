import numpy as np
from pandas import DataFrame

from src import *
from src.nfl.inline_validation import perform_inline_play_action_tests
from src.util.database_loader import DatabaseLoader

logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)

db = DatabaseLoader(get_config('connection_string'))
DEBUG = False
COMMIT_TO_DATABASE = True
SCHEMA = 'controls'


def build_all_weeks_template() -> DataFrame:
    logger.info("Build a 'control' dataset with all seasons and weeks...")
    return db.query_to_df(
        """
        select posteam as team, season, week from controls.play_actions group by posteam, season, week order by posteam, season, week
        """
    )


def build_play_actions_dataset() -> DataFrame:
    logger.info("query and modify play actions data ...")

    play_actions_df = db.query_to_df("""
        WITH play_actions AS (select season,
                                     game_id,
                                     week,
                                     drive,
                                     down,
                                     drive_id,
                                     home_team,
                                     away_team,
                                     posteam,
                                     defteam,
                                     ydstogo,
                                     yards_to_goal,
                                     yards_gained,
                                     play_counter,
                                     action,
                                     game_seconds_remaining,
                                     posteam_score,
                                     posteam_score_post,
                                     defteam_score,
                                     home_score as home_final_score,
                                     away_score as away_final_score,
                                     case when posteam = home_team then home_score else away_score end as posteam_final_score,
                                     case when defteam = home_team then home_score else away_score end as defteam_final_score,
                                     score_differential,
                                     score_differential_post,
                                     offense_yards_gained,
                                     defense_yards_gained,
                                     pass_attempt,
                                     rush_attempt,
                                     kickoff_attempt,
                                     punt_attempt,
                                     field_goal_attempt,
                                     two_point_attempt,
                                     extra_point_attempt,
                                     timeout,
                                     penalty,
                                     qb_spike,
                                     "desc",
                                     row_number() over (partition by -- make sure we are not creating unwanted records
                                         game_id,
                                         drive,
                                         play_counter) as rn
                              from controls.play_actions
                              where action in (
                                               'extra_point',
                                               'field_goal',
                                               'pass',
                                               'rush')
        ),
        next_starting_scores AS (
                 -- intermediate step:
                 -- get the score from the next down into the current record
                 -- assign a unique row_id so we can validate it only occurs once in the final dataset
                 SELECT *,
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL))                             AS row_id,
                        LEAD(posteam_score)
                        OVER (PARTITION BY season, week, posteam, drive ORDER BY play_counter) AS next_starting_score
                 FROM play_actions
                 WHERE rn = 1
        )
        select season,
               game_id,
               week,
               drive,
               down,
               drive_id,
               home_team,
               away_team,
               posteam,
               defteam,
               action,
               posteam_score,
               defteam_score,
               home_final_score,
               away_final_score,
               posteam_final_score,
               defteam_final_score,
               CASE
                   WHEN next_starting_score is not null THEN next_starting_score - posteam_score
                   ELSE posteam_score_post - posteam_score END AS points_gained,
               score_differential,
               score_differential_post,
               offense_yards_gained,
               defense_yards_gained,
               ydstogo,
               yards_to_goal,
               yards_gained,
               play_counter,
               game_seconds_remaining,
               pass_attempt,
               rush_attempt,
               kickoff_attempt,
               punt_attempt,
               field_goal_attempt,
               two_point_attempt,
               extra_point_attempt,
               timeout,
               penalty,
               qb_spike,
               "desc"
        from next_starting_scores
        order by season, week, drive, down
    """)

    perform_inline_play_action_tests(play_actions_df, msg='double checking play actions before save')
    return play_actions_df


def build_game_info_dataset() -> DataFrame:
    logger.info("query and modify game info data ...")

    game_df = db.query_to_df("""
       --home team labels
        select season,
               week,
               home_team                 as team,
               home_score                as team_score,
               home_coach                as team_coach,
               away_team                 as opposing_team,
               away_score                as opposing_score,
               away_coach                as opposing_coach,
               (home_score - away_score) as spread,
               count(*)
    
        from controls.game_info G
        group by season, week, home_team, home_score, away_score, away_team, home_coach, away_coach
        UNION ALL
        --away team labels
        select season,
               week,
               away_team                 as team,
               away_score                as team_score,
               away_coach                as team_coach,
               home_team                 as opposing_team,
               home_score                as opposing_score,
               home_coach                as opposing_coach,
               (away_score - home_score) as spread,
               count(*)
    
        from controls.game_info G
        group by season, week, home_team, home_score, away_score, away_team, home_coach, away_coach
    """)

    game_df.spread = game_df.spread.astype('float')
    game_df['win'] = np.where(
        game_df.spread > 0, 'win',
        np.where(game_df.spread < 0, 'loss', 'tie'))

    # game_df.loc[(game_df.season == 2017) & (game_df.week == 6) & (game_df.team.isin(['DEN', 'NYG']))]

    # fail if there are any group counts > 1
    double_counts = game_df.loc[(game_df['count'].astype(int) > 1)].shape[0]
    assert double_counts == 0

    game_df.spread = game_df.spread.astype('float')
    game_df['win'] = np.where(
        game_df.spread > 0, 'win',
        np.where(game_df.spread < 0, 'loss', 'tie'))

    return game_df


def build_ngs_air_stats() -> DataFrame:
    logger.info("query and modify next gen passing data ...")

    ngs_air_power = db.query_to_df("""
        with base as (
        select season, week, team_abbr as team,
               pass_touchdowns,
               avg_time_to_throw,
               avg_completed_air_yards,
               avg_intended_air_yards,
               avg_air_yards_differential,
               aggressiveness,
               max_completed_air_distance,
               avg_air_yards_to_sticks,
               attempts,
               pass_yards,
               interceptions,
               passer_rating,
               completions,
               completion_percentage,
               expected_completion_percentage,
               completion_percentage_above_expectation,
               avg_air_distance,
               max_air_distance,
            row_number() over (partition by season, week, team_abbr, player_position order by pass_yards desc) as rn
        from controls.nextgen_pass
    --    where season=2016 and week=1 and team_abbr = 'CHI'
        order by team_abbr, player_position, season desc, week )
        select * from base where rn = 1 and week > 0
    """)

    ngs_air_power.drop(columns=['rn'], inplace=True)
    return ngs_air_power


def build_ngs_ground_stats() -> DataFrame:
    logger.info("query and modify next gen rushing data ...")

    ngs_ground_power = db.query_to_df("""
    with base as (
        select season, week, team_abbr as team,
               rush_yards,
               efficiency,
               percent_attempts_gte_eight_defenders,
               avg_time_to_los,
               rush_attempts,
               avg_rush_yards,
               rush_touchdowns,
               player_gsis_id,
               player_first_name,
               player_last_name,
               player_jersey_number,
               player_short_name,
               row_number() over (partition by season, week, team_abbr order by rush_yards desc) as rn
        from controls.nextgen_rush
        order by  team_abbr, season desc, week)
    select * from base where rn = 1 and week > 0
    """)

    ngs_ground_power.drop(columns=['rn'], inplace=True)
    return ngs_ground_power


def build_pbp_events_dataset() -> DataFrame:
    logger.info("query and modify play-by-play player events ...")

    return db.query_to_df("""
        with players as (
            select distinct game_id,  player_id, team from controls.player_participation
        ),
        defensive_events as (
        select pe.season, pe.week, pe.game_id, pp.team, pe.player_id, pe.event, pe.lineup from controls.player_events pe
                 left join players pp on (pp.player_id = pe.player_id and pp.game_id = pe.game_id)
        order by play_id )
        SELECT
            season, week, team, game_id,
            SUM(CASE WHEN event = 'fumble' THEN 1 else 0 END) AS fumble,
         --   SUM(CASE WHEN event = 'own_kickoff_recovery' THEN 1 else 0 END) AS own_kickoff_recovery,
            SUM(CASE WHEN event = 'safety' THEN 1 else 0 END) AS safety,
            SUM(CASE WHEN event = 'tackle' THEN 1 else 0 END) AS tackle,
            SUM(CASE WHEN event = 'qb_hit' THEN 1 else 0  END) AS qb_hit,
         --   SUM(CASE WHEN event = 'touchdown' THEN 1  else 0 END) AS touchdown,
            SUM(CASE WHEN event = 'interception' THEN 1 else 0 END) AS interception,
            SUM(CASE WHEN event = 'sack' THEN 1 else 0 END) AS sack
        FROM defensive_events where week > 0
        group by season, week, team, game_id
        order by season desc, team, week;
    """)


def build_offense_player_stats() -> DataFrame:
    logger.info("query offense player_stats data ...")
    return db.query_to_df("""
        select
            season,
            week,
            team,
            'possession' as side,
            sum(completions) as ps_completions,
            sum(attempts) as ps_attempts,
            sum(passing_yards) as passing_yards,
            sum(passing_tds) as passing_tds,
            sum(passing_air_yards) as passing_air_yards,
            sum(passing_yards_after_catch) as passing_yards_after_catch,
            sum(passing_first_downs) as passing_first_downs,
            avg(passing_epa) as passing_epa,
            sum(passing_2pt_conversions) as passing_2pt_conversions,
            sum(carries) as carries,
            sum(rushing_yards) as rushing_yards,
            sum(rushing_tds) as rushing_tds,
            sum(rushing_first_downs) as rushing_first_downs,
            avg(rushing_epa) as avg_rushing_epa,
            sum(rushing_2pt_conversions) as rushing_2pt_conversions,
            sum(receptions) as receptions,
            sum(targets) as targets,
            sum(receiving_yards) as receiving_yards,
            sum(receiving_tds) as receiving_tds,
            sum(receiving_air_yards) as receiving_air_yards,
            sum(receiving_yards_after_catch) as receiving_yards_after_catch,
            sum(receiving_first_downs) as receiving_first_downs,
            avg(receiving_epa) as avg_receiving_epa,
            sum(receiving_2pt_conversions) as receiving_2pt_conversions,
            sum(special_teams_tds) as special_teams_tds
        from controls.player_stats
        group by season,
                 week,
                 team
        order by season desc, team,  week
    """)


def build_defense_player_stats() -> DataFrame:
    logger.info("query defense player_stats data ...")
    return db.query_to_df("""
    select
        season,
        week,
        team,
        'defense' as side,
        sum(interceptions) as ps_interceptions,
        sum(sacks) as sacks,
        sum(sack_yards) as sack_yards,
        sum(sack_fumbles) as sack_fumbles,
        sum(sack_fumbles_lost) as sack_fumbles_lost,
        sum(rushing_fumbles) as rushing_fumbles,
        sum(rushing_fumbles_lost) as rushing_fumbles_lost,
        sum(rushing_first_downs) as rushing_first_downs,
        sum(receiving_fumbles) as receiving_fumbles,
        sum(receiving_fumbles_lost) as receiving_fumbles_lost
    from controls.player_stats
    group by season,
             week,
             team
    order by season desc, team,  week
    """)

