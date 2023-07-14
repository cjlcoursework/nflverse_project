import numpy as np
import pandas as pd


def summarize_indicators(df, feature_names, threshold):
    indicators = df.loc[(df.Mean > threshold) & df['Feature'].isin(feature_names)].copy()
    indicators['percentage'] = indicators.Mean / indicators.Mean.sum()
    return indicators[['Feature', 'percentage']] \
        .sort_values(by='percentage', ascending=False).pivot_table(columns=['Feature'])


def calculate_power_score(data_df, indicators, column_name):
    weighted_columns = []
    for col in indicators.columns:
        # play_df[f"w_{col}"] = offense_indicators.iloc[0][col]
        w = f"v_{col}"
        weighted_columns.append(w)
        data_df[w] = data_df[col] * indicators.iloc[0][col]

    data_df[column_name] = data_df[weighted_columns].sum(axis=1)
    data_df.drop(columns=weighted_columns, inplace=True)


def build_all_game_weeks(weekly_stats_df: pd.DataFrame) -> pd.DataFrame:
    # there are some missing weeks in the power data.
    # This is probably ok for this application but let's try to backfill some of the data
    # Construct the skeleton as a list of dictionaries
    unique_teams = np.sort(weekly_stats_df['team'].unique())
    unique_seasons = np.sort(weekly_stats_df['season'].unique())
    max_week = weekly_stats_df['week'].max()
    max_weeks = list(range(1, max_week+1))

    skeleton = []
    for team in unique_teams:
        for season in unique_seasons:
            for week in max_weeks:
                skeleton.append(dict(team=team, season=season, week=week))

    # Create the DataFrame from the skeleton
    all_weeks = pd.DataFrame(skeleton)
    return all_weeks


def backfill_missing_weeks(power_df, all_weeks, target_column) -> pd.DataFrame:

    df = power_df[['team', 'season', 'week', target_column]]
    df.sort_values(['team', 'season', 'week'], inplace=True)
    # Create a reference DataFrame with all possible combinations of team, season, and week

    # Merge the reference DataFrame with the original DataFrame to insert missing rows
    df = pd.merge(all_weeks, df, on=['team', 'season', 'week'], how='left')

    # Sort the DataFrame again after merging
    df.sort_values(['team', 'season', 'week'], inplace=True)

    # Use backward fill to propagate values from the next available week
    df[target_column] = df.groupby(['team', 'season'])[target_column].bfill()

    # Use forward fill to propagate values from the previous available week
    df[target_column] = df.groupby(['team', 'season'])[target_column].ffill()

    # Reset the index if needed
    df.reset_index(drop=True, inplace=True)

    return df.drop_duplicates()


def build_power_datasets(weekly_stats_df, summary_data, offense_features, defense_features) ->(pd.DataFrame, pd.DataFrame):

    offense_indicators = summarize_indicators(
        summary_data,
        offense_features, .023)

    defense_indicators = summarize_indicators(
        summary_data,
        defense_features, .023 )

    power_scores_df = weekly_stats_df.copy()

    calculate_power_score(power_scores_df, offense_indicators, 'offense_power')
    calculate_power_score(power_scores_df, defense_indicators, 'defense_power')

    # power_scores_df = power_scores_df[['season', 'week', 'team', 'offense_power', 'defense_power']].drop_duplicates()
    all_weeks = build_all_game_weeks(power_scores_df)

    # offense power scores
    raw_offense_power_df = power_scores_df[['season', 'week', 'team', 'offense_power']].drop_duplicates()
    offense_power_df = backfill_missing_weeks(raw_offense_power_df, all_weeks, target_column='offense_power')

    raw_defense_power_df = power_scores_df[['season', 'week', 'team', 'defense_power']].drop_duplicates()
    defense_power_df = backfill_missing_weeks(raw_defense_power_df, all_weeks, target_column='defense_power')

    return offense_power_df, defense_power_df




if __name__ == '__main__':
    pass