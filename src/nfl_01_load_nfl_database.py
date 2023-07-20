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
# - output directory where we downloaded nflverse files
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
# 
# ![nflverse database](../images/database.png)
# 
# 
# 
# <font color=teal>
# _______________________________________
# </font>
# 

# # <font color=teal>imports</font>
# Most processing is performed in python code, and there's a python module to do everything here without manual

# In[1]:


import logging
import os
import sys

from src import configs

sys.path.append(os.path.abspath("../src"))



# In[2]:


from src.nflverse_transform_job import load_files
from src.pbp_fact import transform_pbp
from src.pbp_participation import transform_pbp_participation
from src.player_stats import transform_player_stats, merge_injuries
from src.player_injuries import prep_player_injuries
from src.player_stats import transform_players
from src.db_utils import load_dims_to_db


# # <font color=teal>housekeeping</font>

# In[3]:


LOAD_TO_DB = True
database_schema = 'controls'

# Get the logger
logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)


# 

# ---
# 
# # <font color=teal>load and transform play by play datasets</font>

# ### <font color="#9370DB">load</font>

# In[4]:


#time
pbp = load_files(data_subdir='pbp')


# ### <font color="#9370DB">transform</font>

# In[5]:


#time
datasets = transform_pbp(pbp)


# 

# ---
# 
# # <font color=teal>load and transform play by play participation datasets</font>

# 
# 

# In[6]:


#time
pbp_participation_df = load_files('pbp-participation')


# ### <font color="#9370DB">transform</font>

# In[7]:


#time
player_df, player_events_df = transform_pbp_participation(
    participation_df=pbp_participation_df,
    player_events=datasets['player_events'])

datasets.update({
    'player_participation': player_df,
    'player_events': player_events_df,
})


# 

# ---
# 
# # <font color=teal>transform player injuries</font>

# ### <font color="#9370DB">load</font>

# In[8]:


#time
injuries_df = load_files('injuries')


# ### <font color="#9370DB">transform</font>

# In[9]:


#time
injuries_df = prep_player_injuries(injuries_df)


# 

# ---
# 
# # <font color=teal>transform player stats</font>

# In[10]:


#time
stats_df = load_files('player-stats')
stats_df = transform_player_stats(stats_df)
stats_df = merge_injuries(player_stats=stats_df, player_injuries=injuries_df)


# 

# ---
# 
# # <font color=teal>direct loads </font>

# ### <font color="#9370DB">adv stats</font>

# In[11]:


#time

advstats_def_df = load_files('advstats-season-def')
advstats_pass_df = load_files('advstats-season-pass')
advstats_rec_df = load_files('advstats-season-rec')
advstats_rush_df = load_files('advstats-season-rush')


# ### <font color="#9370DB">nextgen stats</font>

# In[12]:


#time
next_pass_df = load_files('nextgen-passing')


# In[13]:


#time
next_rec_df = load_files('nextgen-receiving')


# In[14]:


#time
next_rush_df = load_files('nextgen-rushing')


# ### <font color="#9370DB">players</font>

# In[15]:


#time
players_df = load_files('players')
players_df = transform_players(players_df)


# ---
# 
# # <font color=teal>store to database so we can perform some SQL operations</font>

# In[16]:


def load_all_datasets_to_db(data: dict):
    data['schema'] = database_schema
    load_dims_to_db(data)


# In[17]:


#time
if LOAD_TO_DB:
    datasets.update({
        'players': players_df,
        'player_stats': stats_df,
        'adv_stats_def': advstats_def_df,
        'adv_stats_pass': advstats_pass_df,
        'adv_stats_rec': advstats_rec_df,
        'adv_stats_rush': advstats_rush_df,
        'nextgen_pass': next_pass_df,
        'nextgen_rec': next_rec_df,
        'nextgen_rush': next_rush_df
    })
    load_all_datasets_to_db(datasets)


# In[17]:




