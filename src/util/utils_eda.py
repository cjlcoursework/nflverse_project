import numpy as np
import pandas as pd
import seaborn
import xgboost as xgb
from matplotlib import pyplot as plt
from pandas import DataFrame
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay, precision_score, recall_score, f1_score
from typing import Set, Any


def print_f1_scores(y_pred: Any, y_test: Any):
    """
    Print precision, recall, and F1 scores
    Parameters:
        y_pred: The predicted values
        y_test: The test values
    """
    # Calculate precision, recall, and F1 scores
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)

    print("")
    print("Precision:", precision)
    print("Recall:", recall)
    print("F1-score:", f1)


def plot_accuracy(r: Any):
    """
    Plot accuracy and validation accuracy over epochs
    Parameters:
        r: The history from a model fit
    """
    # Evaluate the model
    plt.plot(r.history['accuracy'], label='accuracy')
    plt.plot(r.history['val_accuracy'], label='val-accuracy')
    plt.xlabel("epochs")
    plt.ylabel("accuracy")
    plt.legend()
    plt.show()


def plot_confusion_matrix(y_pred: Any, y_test: Any):
    """
    Plot confusion matrix
    Parameters:
        y_pred: The predicted values

    """
    cm = confusion_matrix(y_pred=y_pred, y_true=y_test)
    cm_display = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=[False, True])
    cm_display.plot(cmap='Blues')
    plt.show()


def plot_loss(r):
    """
    Plot loss and validation loss over epochs
    Parameters:
        r: The history from a model fit
    """
    plt.plot(r.history['loss'], label='loss')
    plt.plot(r.history['val_loss'], label='val-loss')
    plt.xlabel("epochs")
    plt.ylabel("loss")
    plt.legend()
    plt.show()


def plot_correlations(scores: Any, features: Any, title: str):
    """
    Plot feature importance
    Parameters:
        scores: The scores
        features: The features
        title: The title
    """
    # Plot feature importance
    plt.figure(figsize=(10, 10))
    plt.barh(range(len(scores)), scores, align='center')
    plt.yticks(range(len(features)), features)
    plt.xlabel('Score')
    plt.ylabel('Column')
    plt.title(title)
    plt.show()


def plot_heatmap(df: DataFrame, drop_columns: Set):
    """
    Plot heatmap
    Parameters:
        df: The DataFrame
        drop_columns: The columns to drop
    """
    numeric_df = df.select_dtypes(include='number').drop(columns=drop_columns)
    correlation_matrix = numeric_df.corr()
    plt.figure(figsize=(15, 10))
    plt.imshow(correlation_matrix, cmap='coolwarm', interpolation='nearest')
    plt.colorbar()
    plt.title('Heatmap')

    new_font_size = 6  # Reduce by about 1/3rd

    # Set x-axis and y-axis labels
    plt.xticks(np.arange(len(numeric_df.columns)), numeric_df.columns, rotation=45, fontsize=new_font_size)

    # Calculate the new font size for y-axis labels
    current_font_size = plt.rcParams['ytick.labelsize']

    plt.yticks(np.arange(len(numeric_df.columns)), numeric_df.columns, fontsize=new_font_size)
    plt.show()


def hist_charts(df: DataFrame):
    """
    Plot histograms
    """
    num_cols = 4
    num_rows = (len(df.columns) + num_cols - 1) // num_cols

    # Generate separate histograms using seaborn for each numeric column
    fig, axes = plt.subplots(num_rows, num_cols, figsize=(15, 3 * num_rows))
    for i, column in enumerate(df.columns):
        row = i // num_cols
        col = i % num_cols
        seaborn.set(style="ticks")
        seaborn.histplot(data=df[column], bins=30, kde=True, ax=axes[row, col])
        axes[row, col].set_title(f"Histogram of {column}")

    # Adjust spacing between subplots
    plt.tight_layout()


def calc_feature_importance(X: pd.DataFrame, y: pd.Series, top_n=30) -> (pd.DataFrame, Set):
    """
    Calculate feature importance scores
    Parameters:
        X: The features
        y: The target
        top_n: The number of features to return
    Returns:
        DataFrame: The feature importance scores
        Set: The top features
    """
    # Create an XGBoost model
    model = xgb.XGBRegressor()

    # Fit the model
    model.fit(X, y)

    # Get feature importance scores
    importance_scores = model.feature_importances_

    # Sort feature importance scores
    sorted_indices = importance_scores.argsort()[::-1]
    sorted_scores = importance_scores[sorted_indices]
    feature_names = X.columns[sorted_indices]

    # Get the top 'n' feature importance scores and names
    top_scores = sorted_scores[:top_n]
    top_features = feature_names[:top_n]
    df = pd.DataFrame({'y': top_features, 'corr': top_scores})

    return df, set(top_features)


def correlate_to_target(df: pd.DataFrame, target_column: str, top_n: int) -> (pd.DataFrame, Set):
    """
    Calculate correlation coefficients to the target column
    Parameters:
        df: The DataFrame
        target_column: The target column
        top_n: The number of features to return
    Returns:
        DataFrame: The correlation coefficients
        Set: The top features
    """
    # Calculate correlation matrix
    correlation_matrix = df.corr()

    # Filter and sort correlation coefficients by absolute value
    corr_df = pd.DataFrame(correlation_matrix.abs().unstack().sort_values(ascending=False))
    corr_df.reset_index(inplace=True)
    corr_df.columns = ['x', 'y', 'corr']

    not_self_correlated = (corr_df.y != target_column)
    win_correlated = (corr_df.x == target_column)

    df = corr_df.loc[win_correlated & not_self_correlated] \
        .sort_values(by='corr', ascending=False) \
        .drop(columns=['x']) \
        .head(top_n) \
        .copy()

    s = set(df['y'].values)

    return df, s
