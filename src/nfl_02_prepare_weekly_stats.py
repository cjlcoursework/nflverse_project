#!/usr/bin/env python
# coding: utf-8

# 
# <font color=teal>
# _______________________________________
# </font>
# 
# 
# ### <font color=teal>Goal:</font>
# 
# - Build a dimensional version of the nflverse data, separating data with different cardinalities
# 
# ### <font color=teal>Input:</font>
# 
# -- Merge statistics data from all sources and merge into a single dataset - to be used in feature selection
# 
# 
# ### <font color=teal>Steps:</font>
# - Split data into smaller dimensions .e.g game info vs play info vs play analytics, etc.
# - Insert data into DB tables
# - store to a database for further experimentation
# 
# ### <font color=teal>Output:</font>
# 
# - DB tables
# 
# ![nflverse database](../images/database.png)
# 
# 
# 
# <font color=teal>
# _______________________________________
# </font>
# 

# Goal:
# - Merge selected stats from all sources into a single dataset - aggregated at the season and week and team level - to be used in feature selection
# 
# Inputs:
#     rollup all to season, week, team cardinality, and expand to one record per team
#         - game_info - expand so there is one record for each team as opposed to one record for each game with home and away teams
#         - player_stats
#         - nextgen_pass  -- nextgen_rec is going to be largely redundant to pass for our purposes
#         - nextgen_rush
#         - player_events -- events and milestones embedded in the pbp data
#         - player_stats -
#         - play_analytics - probabilities and stats embedded in the pbp data - exand to one record per team
#     merge all together into weekly stats table
#     iteratively review distributions and keep or drop columns based on our needs
#     impute
#     adjust dtypes for columns that should be numeric
# 
# Output
#     - nfl_weekly_stats.parquet
# 

# # <font color=teal>imports</font>

# In[1]:


import os
import sys
sys.path.append(os.path.abspath("../src"))


# In[2]:


from  src import *
from src.utils import assert_and_alert
import numpy as np
import pandas as pd

logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)


# # <font color=teal>housekeeping</font>

# In[3]:


db = database_loader.DatabaseLoader(get_config('connection_string'))
DEBUG=False
COMMIT_TO_DATABASE=True
SCHEMA='controls'


# In[4]:


# create a 'skeleton' of all the team weeks in all seasons
#  we'll use this to backfill any missing stats with the nearest week for a given team
all_team_weeks = db.query_to_df(
    """select posteam as team, season, week from controls.play_actions group by posteam, season, week order by posteam, season, week"""
)

all_team_weeks.head()


# # <font color=teal>game info data</font>
# Aggregated up from the play-by-play dataset.
# 
# We want each team to have its own record for each season and week.
# 
# So for any given game there will be two records, one for the home team having its stats, and another for the away team - having the opposite stats

# In[5]:


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

play_actions_df.head()


# In[6]:


from src.inline_validation import perform_inline_play_action_tests

perform_inline_play_action_tests(play_actions_df)


# In[7]:


# We want each team to have a record for each season and week.

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
    game_df.spread>0, 'win',
    np.where(game_df.spread<0, 'loss', 'tie') )

game_df.head()


# ### <font color="#9370DB">A single game should have two 'opposite' records</font>
# let's check that out for the 2017 match between DEN and NYG

# In[8]:


game_df.loc[(game_df.season==2017) & (game_df.week==6) & (game_df.team.isin(['DEN', 'NYG']))]


# ### <font color="#9370DB">There should be no team with two records for any give week</font>
# let's validate that

# In[9]:


# fail if there are any group counts > 1
double_counts = game_df.loc[(game_df['count'].astype(int) > 1)].shape[0]
assert double_counts == 0


# # <font color=teal>next gen stats passing<font/>
# group by <font color=red>season, week, team</font> ( and top-passing-player_position )

# In[10]:


#time
# team level stats by season and week and player, and position from 2016 to 2022
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
ngs_air_power.head()


# ## <font color=teal>next gen stats rushing<font/>
# group by <font color=red>season, week, team</font>

# In[11]:


#time
# team level stats by season and week and player, and position from 2016 to 2022
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
""" )


ngs_ground_power.drop(columns=['rn'], inplace=True)
ngs_ground_power.head()


# # <font color=teal>play-by-play events<font/>
# players are called out for certain events like fumbles, touchdowns, etc. in play-by-play
# we already picked these out during the transform step,
#   and expanded so that each team has its own records irrespective of the opposing team played.
# Now we pivot and sum all events by  <font color=red>season, week, team</font>

# In[12]:


#time
pbp_events = db.query_to_df("""
-- players: select unique players and their teams from specific games
-- defensive_events: merge players teams into events and keep only defensive events
-- rollup individual player events to a team sum grouped by season, week, game
-- players: select unique players and their teams from specific games
-- defensive_events: merge players teams into events and keep only defensive events
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

pbp_events.head()


# # <font color=teal>player stats<font/>
# Each player's stats by are collected by game and play
# For this dimension reduction exercise we roll up to <font color=red>season, week, team</font>

# In[13]:


#time
possession_stats = db.query_to_df("""
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

possession_stats.head()


# In[14]:


defense_stats = db.query_to_df("""
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

defense_stats.head()


# # <font color=teal>merge stats datasets ...</font>
# We want one record for each season, week and team.
# 
# The metrics themselves pivot horizontally as columns - all the metrics we created above become columns in this final dataset
# so we expect that with each merge the number of columns grows, but the row count stays the same
# 
# Since we are using this dataset for dimensionality reduction it's ok if we loose a few rows on the join.
# 

# 

# ### <font color="#9370DB">back and forward fills</font>

# In[15]:


from src.build_power_scores import backfill_missing_metrics

ngs_air_power = backfill_missing_metrics(ngs_air_power, all_team_weeks, 'ngs_air_power')
ngs_ground_power = backfill_missing_metrics(ngs_ground_power, all_team_weeks, 'ngs_ground_power')
pbp_events = backfill_missing_metrics(pbp_events, all_team_weeks, 'pbp_events')
defense_stats = backfill_missing_metrics(defense_stats, all_team_weeks, 'defense_stats')
possession_stats = backfill_missing_metrics(possession_stats, all_team_weeks, 'possession_stats')


# ### <font color="#9370DB">helper functions</font>

# In[16]:


from pandas import DataFrame

def calc_coverage(title: str, df: DataFrame):
    first = df.season.min()
    last = df.season.max()
    first_wk = df.week.min()
    last_wk = df.week.max()
    seasons = df.season.nunique()
    print(f"Shape of {title:30}:  {df.shape},\t Contains {seasons} seasons, starting with {first} and ending in {last} min week: {first_wk}, max week : {last_wk}")

def print_columns(title, df):
    print(f"\n---------\n{title.strip()} colums")
    for col in df.columns:
        print(col)


# ### <font color="#9370DB">get shapes before merge</font>

# In[17]:


# calc_coverage("Team analytics ", team_analytics)
calc_coverage("ngs_air_power  ", ngs_air_power)
calc_coverage("ngs_ground_power ", ngs_ground_power)
calc_coverage("pbp_events  ", pbp_events)
calc_coverage("defense_stats  ", defense_stats)
calc_coverage("possession_stats  ", possession_stats)
calc_coverage("game info  ", game_df)


# In[18]:


if DEBUG:
    print_columns("ngs_air_power  ", ngs_air_power)
    print_columns("ngs_ground_power ", ngs_ground_power)
    print_columns("pbp_events  ", pbp_events)
    print_columns("defense_stats  ", defense_stats)
    print_columns("possession_stats  ", possession_stats)
    print_columns("game info  ", game_df)


# ### <font color="#9370DB">merge offense stats</font>

# In[19]:


# merge possession_stats + ngs_pass, ngs_rush
print("merge all offense stats")
starting_shape = possession_stats.shape
possession_stats = pd.merge(possession_stats, ngs_air_power, on=['season', 'week', 'team'])
possession_stats = pd.merge(possession_stats, ngs_ground_power, on=['season', 'week', 'team'])
print(f"possession_stats before: {starting_shape}, after: {possession_stats.shape}")

assert_and_alert(starting_shape[0] == possession_stats.shape[0], msg=f"possession_stats before: {starting_shape}, after: {possession_stats.shape}")

possession_stats = pd.merge(possession_stats, game_df, on=['season', 'week', 'team'])

print(f"possession_stats before: {starting_shape}, after game_info: {possession_stats.shape} -ok - these look like garbage rows and it's only 149 ")

possession_stats.head()


# ### <font color="#9370DB">merge defense stats</font>

# In[20]:


# merge defensive_stats _ layer_events
print("merge all defense stats")
starting_shape = defense_stats.shape
defense_stats = pd.merge(defense_stats, pbp_events, on=['season', 'week', 'team'])
print(f"possession_stats before: {starting_shape}, after: {defense_stats.shape}")

assert_and_alert(starting_shape[0] == defense_stats.shape[0], msg=f"possession_stats before: {starting_shape}, after: {defense_stats.shape}")

defense_stats = pd.merge(defense_stats, game_df, on=['season', 'week', 'team'])

print(f"defense_stats before: {starting_shape}, after game_info: {defense_stats.shape} -ok - these look like garbage rows, not needed for this application ")

defense_stats.head()


# ### <font color="#9370DB">verify that there are no team weeks with more than one record</font>

# In[21]:


#time

def check_for_merge_columns(merged):
    overlaps = 0
    for col in merged.columns:
        if str(col).endswith("_y") or str(col).endswith("_x") or str(col) == "rn":
            print(col)
            overlaps += 1

    assert overlaps == 0
    print("ok")

check_for_merge_columns(defense_stats)
check_for_merge_columns(possession_stats)


# # <font color=teal>review and impute our new dataset</font>

# ### <font color="#9370DB">review our dataset</font>

# ### <font color="#9370DB">impute missing values</font>

# In[22]:


def calc_percentage_missing(df):
    rows_count = df.shape[0]
    missing = df.isnull().sum().sort_values(ascending=False)
    perc_missing = (missing / rows_count) * 100
    perc_missing = perc_missing.reset_index()
    perc_missing.columns = ['column', 'percentage_missing']

    print(perc_missing)


# In[23]:


assert_and_alert(0 == possession_stats.isnull().sum().sum(), msg=f"found unexpected nulls in possession_stats")
assert_and_alert(0 == defense_stats.isnull().sum().sum(), msg=f"found unexpected nulls in possession_stats")


# ### <font color="#9370DB">interactively review distributions and decide which columns to keep</font>
# 
# #### helper functions

# In[24]:


import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns


def hist_charts(numeric_columns):
    # Calculate the number of rows and columns for the grid
    num_cols = 4
    num_rows = (len(numeric_columns.columns) + num_cols - 1) // num_cols

    # Generate separate histograms using seaborn for each numeric column
    fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, 3*num_rows))
    for i, column in enumerate(numeric_columns.columns):
        row = i // num_cols
        col = i % num_cols
        sns.set(style="ticks")
        sns.histplot(data=numeric_columns[column], bins=30, kde=True, ax=axes[row, col])
        axes[row, col].set_title(f"Histogram of {column}")

    # Adjust spacing between subplots
    plt.tight_layout()

    # Show the plots
    plt.show()


# #### charts

# ### <font color="#9370DB">convert object columns to int</font>

# ### <font color="#9370DB">store data</font>

# In[25]:


perform_inline_play_action_tests(play_actions_df, msg='double checking play actions before save')


# In[26]:


#time
from src.db_utils import store_df

store_df(possession_stats, "nfl_weekly_offense", db=db if COMMIT_TO_DATABASE else None, schema=SCHEMA)
store_df(defense_stats,    "nfl_weekly_defense", db=db if COMMIT_TO_DATABASE else None, schema=SCHEMA)
store_df(play_actions_df,  "nfl_play_actions", db=db if COMMIT_TO_DATABASE else None, schema=SCHEMA)

