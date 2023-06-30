import pandas as pd
import numpy as np

configurations = {
    'connection_string': 'postgresql://postgres:chinois1@localhost',
    'positions_data': "/Users/christopherlomeli/Source/courses/datascience/Springboard/capstone/NFL/nfl_capstone/data/raw/positions.csv"
}

def get_config(name, default=None):
    if name in configurations:
        return configurations[name]
    return default


