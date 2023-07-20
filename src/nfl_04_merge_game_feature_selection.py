#!/usr/bin/env python
# coding: utf-8

# <font color=teal>
# _______________________________________
# </font>
# 
# 
# ### <font color=teal>Goal:</font>
# 
# - Merge play actions and offense/defense power scores into a play by play dataset focused on play-calling
# 
# ### <font color=teal>Input:</font>
# 
# - pbp_actions.parquet
# - defense_power.parquet
# - offense_power.parquet
# 
# 
# ### <font color=teal>Steps:</font>
# - merge offense and defense scores into each play based on which team offense and defense
# - save the final play-calling dataset
# 
# 
# ### <font color=teal>Output:</font>
# 
# - store_df(df, "nfl_pbp_play_calls", db=db if COMMIT_TO_DATABASE else None, schema=SCHEMA)
# -
# - store_df(games_df, "nfl_pbp_game_stats", db=db if COMMIT_TO_DATABASE else None, schema=SCHEMA)
# 
# 
# 
# <font color=teal>
# _______________________________________
# </font>
# 

# 
# ### <font color=teal>imports<font/>

# In[16]:


import os
import sys

sys.path.append(os.path.abspath("../src"))


# In[ ]:


from matplotlib import pyplot as plt
import warnings
from src import *

warnings.filterwarnings('ignore')


# ### <font color=teal>housekeeping<font/>

# In[ ]:


warnings.filterwarnings('ignore')

logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)


# ### <font color=teal>settings<font/>

# In[20]:


db = database_loader.DatabaseLoader(get_config('connection_string'))
DEBUG=False
COMMIT_TO_DATABASE=True
SCHEMA='controls'

data_directory = get_config('data_directory')

plt.style.use('seaborn-darkgrid')


# #### <font color=teal>load play_actions<font/>

# In[21]:


#time

full_path = os.path.join(data_directory, "nfl_play_actions.parquet")
pbp_actions_df = pd.read_parquet(full_path)
pbp_actions_df.head()


# #### load offense stats

# In[22]:


#time
full_path = os.path.join(data_directory, "nfl_weekly_offense_ml.parquet")
offense_powers_df = pd.read_parquet(full_path)
offense_powers_df = offense_powers_df[['season', 'week', 'team', 'offense_power']]
offense_powers_df.head()


# #### load defense stats

# In[23]:


#time
full_path = os.path.join(data_directory, "nfl_weekly_defense_ml.parquet")
defense_powers_df = pd.read_parquet(full_path)
defense_powers_df = defense_powers_df[['season', 'week', 'team', 'defense_power']]
defense_powers_df.head()


# ##### merge into play actions: team in position's offense power and defense power (offense_op, offense_dp)

# In[24]:


from src.utils import assert_and_alert
from src.inline_validation import perform_inline_play_action_tests


def drop_extras(df: pd.DataFrame):
    drops = ['team']
    for col in df.columns.values:
        if str(col).endswith("_y") or str(col).endswith("_x"):
            drops.append(col)
    if len(drops) > 0:
        df.drop(columns=drops, inplace=True)


def merge_powers(action_df, powers_df, left_on, renames=None, msg='play_counter'):
    expected_shape = action_df.shape
    _df = pd.merge(action_df, powers_df, left_on=left_on, right_on=['season', 'week', 'team']).drop_duplicates()
    drop_extras(_df)
    _df.rename(columns=renames, inplace=True)
    perform_inline_play_action_tests(_df, msg=msg)
    assert_and_alert(expected_shape[0] == _df.shape[0],
                     msg=f"merge of actions to offense power changed the row count {pbp_actions_df.shape} + {offense_powers_df.shape} ==> {_df.shape}")
    return _df



# In[25]:


df = merge_powers(pbp_actions_df, offense_powers_df, left_on=['season', 'week', 'posteam'],
                  renames={'offense_power': 'offense_op'}, msg="merging offense_OP")

df = merge_powers(df, defense_powers_df, left_on=['season', 'week', 'posteam'], renames={'defense_power': 'offense_dp'},
                  msg="merging offense_DP")

df = merge_powers(df, offense_powers_df, left_on=['season', 'week', 'defteam'], renames={'offense_power': 'defense_op'},
                  msg="merging defense_OP")

df = merge_powers(df, defense_powers_df, left_on=['season', 'week', 'defteam'], renames={'defense_power': 'defense_dp'},
                  msg="merging defense_DP")



# In[39]:


import numpy as np
import pandas as pd

# Assuming you have a DataFrame named 'nfl_pbp_play_calls' from 'controls' namespace
df['point_spread'] = df['posteam_final_score'] - df['defteam_final_score']
# Group by the desired columns and calculate aggregations
grouped_df = df.groupby(['season', 'week', 'game_id', 'posteam', 'defteam']).agg(
    drive_count=('drive', 'count'),
    first_downs=('down', lambda x: (x == 1).sum()),
    point_spread=('point_spread', 'max'),  # Calculate the point spread explicitly
    team_final_score=('posteam_final_score', 'max'),
    opposing_team_final_score=('defteam_final_score', 'max'),
    yards_gained=('yards_gained', 'sum'),
    pass_attempts=('pass_attempt', 'sum'),
    rush_attempts=('rush_attempt', 'sum'),
    kickoff_attempt=('kickoff_attempt', 'sum'),
    punt_attempt=('punt_attempt', 'sum'),
    field_goal_attempt=('field_goal_attempt', 'sum'),
    two_point_attempt=('two_point_attempt', 'sum'),
    extra_point_attempt=('extra_point_attempt', 'sum'),
    timeout=('timeout', 'sum'),
    penalty=('penalty', 'sum'),
    qb_spike=('qb_spike', 'sum'),
    team_offense_power=('offense_op', 'mean'),
    team_defense_power=('offense_dp', 'mean'),
    opposing_team_offense_power=('defense_op', 'mean'),
    opposing_team_defense_power=('defense_dp', 'mean')
)

# Reset the index to transform the grouped DataFrame back to a regular DataFrame
grouped_df.reset_index(inplace=True)

# Select the desired columns for the final result
games_df = grouped_df[['season', 'week', 'game_id', 'posteam', 'defteam',
                        'team_offense_power', 'team_defense_power', 'opposing_team_offense_power', 'opposing_team_defense_power',
                        'point_spread',  'drive_count', 'first_downs', 'team_final_score',
                        'opposing_team_final_score', 'yards_gained', 'pass_attempts', 'rush_attempts',
                        'kickoff_attempt', 'punt_attempt', 'field_goal_attempt', 'two_point_attempt',
                        'extra_point_attempt', 'timeout', 'penalty', 'qb_spike']]

games_df.rename(columns={'posteam': 'team', 'defteam': 'opposing_team'}, inplace=True)

# Create a new column 'loss_tie_win' based on conditions
games_df['loss_tie_win'] = np.where(
    games_df['point_spread'] > 0, 2,
    np.where(
        games_df['point_spread'] < 0, 0, 1 )
)

games_df['team_power_sum'] = games_df['team_offense_power'] + games_df['team_defense_power']
games_df['opposing_team_power_sum'] = games_df['opposing_team_offense_power'] + games_df['opposing_team_defense_power']
games_df['power_difference'] = games_df['team_power_sum'] - games_df['opposing_team_power_sum']
games_df['point_spread'] = games_df['point_spread'].astype('float')
games_df[['team_offense_power', 'team_defense_power', 'opposing_team_offense_power', 'opposing_team_defense_power',  'team_power_sum' ,'opposing_team_power_sum', 'power_difference' ]].head()


# ##### merge into play actions: team on defense offense power and defense power (defense_op, defense_dp)

# #### save features dataset

# In[40]:


#time
from src.db_utils import store_df

# store_df(df, "nfl_pbp_play_calls", db=db if COMMIT_TO_DATABASE else None, schema=SCHEMA)
store_df(games_df, "nfl_pbp_game_stats", db=db if COMMIT_TO_DATABASE else None, schema=SCHEMA)


# ---
