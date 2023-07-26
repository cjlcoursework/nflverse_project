import os
import pandas as pd
from pandas import DataFrame

import tensorflow as tf
from tensorflow.keras import Sequential
from tensorflow.keras import regularizers
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.optimizers.legacy import Adam
from tensorflow.keras.models import load_model



from src import configs
from src.configs import get_config


def load_2022_data() -> DataFrame:
    file_name = get_config('game_stats')
    data_directory = get_config('data_directory')
    input_path = os.path.join(data_directory, f"{file_name}.parquet")
    assert os.path.exists(input_path)
    stats_df = pd.read_parquet(input_path)
    return stats_df.loc[stats_df['season'] == 2022]


def load_win_loss_model():
    model_directory = get_config('model_directory')
    model_name = get_config('experiment_2_model')
    full_path = os.path.join(model_directory, f'{model_name}.h5')
    loaded_model = tf.keras.models.load_model(full_path)
    loaded_model.summary()
    return loaded_model


def main():
    model = load_win_loss_model()
    stats_df = load_2022_data()




    print(stats_df.head())
    print(model.summary())



if __name__ == '__main__':
    main()