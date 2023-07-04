import pandas as pd
from pandas import DataFrame

from NFLVersReader.src.nflverse_clean.configs import get_config
from utils import assert_and_alert, impute_columns, assert_not_null, validate_positions
from logging_config import confgure_logging
import warnings

warnings.filterwarnings('ignore')
logger = confgure_logging("pbp_logger")


def load_advstats():
    advstats_df = pd.read_csv(get_config('advstats'), low_memory=False)
    return advstats_df
