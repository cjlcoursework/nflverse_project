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
# - Download data from the wonderful nflverse site
# 
# ### <font color=teal>Input:</font>
# 
# - nflverse :: [NFLVerse Data](https://github.com/nflverse/nflverse-data/releases)
# 
# 
# ### <font color=teal>Steps:</font>
# - download for the list of datasets we want
# - store without any transformation
# 
# ### <font color=teal>Output:</font>
# 
# - files at rest
# 
# 
# ![nflverse downloads](../images/output_directory.png)
# 
# 
# 
# <font color=teal>
# _______________________________________
# </font>
# 

# ## <font color=teal>imports</font>

# In[ ]:


import logging
import os
import sys

from src import configs

sys.path.append(os.path.abspath("../src"))


# In[ ]:


from src.nflverse_reader_job import URLReader


# ## <font color=teal>housekeeping</font>

# In[ ]:


LOAD_TO_DB = True
database_schema = 'controls'

# Get the logger
logger = configs.configure_logging("pbp_logger")
logger.setLevel(logging.INFO)


# ## <font color=teal>read from nflverse<font/>
# Read data and store immediately as raw without transformation or change

# In[ ]:


#time
reader = URLReader(start_year=2017, last_year=2022, file_type='parquet')
urls = reader.download()


# 
