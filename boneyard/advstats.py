import pandas as pd
from pandas import DataFrame

from src import *
import warnings

warnings.filterwarnings('ignore')
logger = configure_logging("pbp_logger")


def load_advstats():
    advstats_df = pd.read_csv(get_config('advstats'), low_memory=False)
    return advstats_df
