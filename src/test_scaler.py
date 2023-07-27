from sklearn.preprocessing import MinMaxScaler, RobustScaler

import os
import pandas as pd

import tensorflow as tf
from tensorflow.keras import Sequential
from tensorflow.keras.models import load_model
from src import configs
from src.configs import get_config
from src.nfl_04_merge_game_features import load_file
from src.util.database_loader import DatabaseLoader
from src.util.utils import assert_and_alert
from src.configs import get_config
from src.nfl_04_merge_game_features import load_file

def load_data(season, week=1):
    df = load_file(get_config('data_directory'), get_config('game_stats'))
    return df.loc[(df['season'] == season) & (df['week'] == week)]

def load_win_loss_model():
    model_directory = get_config('model_directory')
    model_name = get_config('experiment_2_model')
    full_path = os.path.join(model_directory, f'{model_name}.h5')
    loaded_model = tf.keras.models.load_model(full_path)
    loaded_model.summary()
    return loaded_model


def scale_numeric_columns(df, scaler):
    # Convert the single row to a DataFrame with one row and appropriate column names
    # df = pd.DataFrame([row], columns=['col1', 'col2', 'col3', 'col4', 'col5', 'col6','col7', 'col8', 'col9','col10', 'col11'])  # Replace 'col1', 'col2', 'col3', ... with your actual column names

    # Scale the DataFrame using the provided scaler
    features = scaler.fit_transform(df)

    # Convert the scaled features back to a DataFrame
    X = pd.DataFrame(features, columns=df.columns)
    return X


if __name__ == '__main__':
    sample_row = [22.0, 27.0, 2.0, 5.0, 99.264706, 144.230769, 2.0, 5.0, 7.683945, 43.251201, 5.169097]

    # Load the trained model

    model = load_win_loss_model()  # Assuming you have saved the model using model.save('model.h5')

    # Load the scaler used during training
    scaler = MinMaxScaler()  # Initialize the scaler (you can load it from disk if you saved it during training)

    # Scale the sample row using the loaded scaler
    df = load_data(2022, 1)

    sample_row_scaled = scale_numeric_columns(df[configs.ml_win_lose_features], scaler)

    # Make predictions using the model
    predictions = model.predict(sample_row_scaled)

    print(predictions)
