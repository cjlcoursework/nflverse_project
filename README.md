# nflverse_sdk

├── README.md
├── __init__.py
├── __pycache__
│   └── __init__.cpython-39.pyc
├── boneyard
│   ├── advstats.py
│   ├── clean_player_stats.py
│   ├── gzip_encoder.py
│   ├── keras_generator.py
│   ├── nflverse_data_job.ipynb
│   ├── nflverse_reader_util.py
│   ├── pbp_dim_field_goal.py
│   ├── pbp_dim_fumble.py
│   ├── pbp_dim_kickoff.py
│   ├── pbp_dim_pass.py
│   ├── pbp_participation.ipynb
│   ├── player_stats.ipynb
│   ├── pytorch_generator.py
│   ├── recon_players_stats.py
│   ├── snappy_encoder.py
│   └── test_nflverse_reader.py
├── data
│   ├── internal
│   │   └── positions.csv
│   └── nfl
│       ├── nfl_ml_offense_stats.parquet
│       ├── nfl_pbp_game_stats.parquet
│       ├── nfl_pbp_play_calls.parquet
│       ├── nfl_play_actions.parquet
│       ├── nfl_weekly_defense.parquet
│       ├── nfl_weekly_defense_ml.parquet
│       ├── nfl_weekly_offense.parquet
│       └── nfl_weekly_offense_ml.parquet
├── doc
│   ├── AWS ML Notes
│   └── nflverse_schemas
│       ├── clean_player_names.csv
│       ├── dictionary_combine.csv
│       ├── dictionary_contracts.csv
│       ├── dictionary_depth_charts.csv
│       ├── dictionary_draft_picks.csv
│       ├── dictionary_espn_qbr.csv
│       ├── dictionary_ff_playerids.csv
│       ├── dictionary_ff_rankings.csv
│       ├── dictionary_ffopps.csv
│       ├── dictionary_injuries.csv
│       ├── dictionary_nextgenstats.csv
│       ├── dictionary_participation.csv
│       ├── dictionary_pbp.csv
│       ├── dictionary_pfr_passing.csv
│       ├── dictionary_playerstats.csv
│       ├── dictionary_rosters.csv
│       ├── dictionary_schedules.csv
│       ├── dictionary_snap_counts.csv
│       ├── dictionary_trades.csv
│       ├── logo.svg
│       ├── readme.md
│       ├── social_preview.png
│       └── social_preview.svg
├── images
│   ├── database.png
│   ├── nfl.png
│   └── output_directory.png
├── model
│   └── nfl_features_model.keras
├── notebooks
│   ├── custom.css
│   ├── nfl_00_nflverse_output.ipynb
│   ├── nfl_01_db_dimensions.ipynb
│   ├── nfl_02_nfl_weekly_stats.ipynb
│   ├── nfl_03_nfl_defense_power_score.ipynb
│   ├── nfl_03_nfl_offense_power_score.ipynb
│   ├── nfl_04_amerge_play_powers.ipynb
│   ├── nfl_05_pbp_team_games.ipynb
│   └── saves
│       ├── nfl_05_ml_features.ipynb
│       ├── nfl_06_ml_playcall.ipynb
│       ├── play_call.ipynb
│       ├── sandbox.ipynb
│       ├── save_nfl_02_nfl_defense_power_score.ipynb
│       └── save_nfl_02_nfl_offense_power_score.ipynb
├── output
│   ├── advstats-season-def
│   │   └── advstats-season-def.parquet
│   ├── advstats-season-pass
│   │   └── advstats-season-pass.parquet
│   ├── advstats-season-rec
│   │   └── advstats-season-rec.parquet
│   ├── advstats-season-rush
│   │   └── advstats-season-rush.parquet
│   ├── injuries
│   │   ├── injuries_2016.parquet
│   │   ├── injuries_2017.parquet
│   │   ├── injuries_2018.parquet
│   │   ├── injuries_2019.parquet
│   │   ├── injuries_2020.parquet
│   │   ├── injuries_2021.parquet
│   │   └── injuries_2022.parquet
│   ├── nextgen-passing
│   │   ├── nextgen-passing_2016.csv.gz
│   │   ├── nextgen-passing_2017.csv.gz
│   │   ├── nextgen-passing_2018.csv.gz
│   │   ├── nextgen-passing_2019.csv.gz
│   │   ├── nextgen-passing_2020.csv.gz
│   │   ├── nextgen-passing_2021.csv.gz
│   │   └── nextgen-passing_2022.csv.gz
│   ├── nextgen-receiving
│   │   ├── nextgen-receiving_2016.csv.gz
│   │   ├── nextgen-receiving_2017.csv.gz
│   │   ├── nextgen-receiving_2018.csv.gz
│   │   ├── nextgen-receiving_2019.csv.gz
│   │   ├── nextgen-receiving_2020.csv.gz
│   │   ├── nextgen-receiving_2021.csv.gz
│   │   └── nextgen-receiving_2022.csv.gz
│   ├── nextgen-rushing
│   │   ├── nextgen-rushing_2016.csv.gz
│   │   ├── nextgen-rushing_2017.csv.gz
│   │   ├── nextgen-rushing_2018.csv.gz
│   │   ├── nextgen-rushing_2019.csv.gz
│   │   ├── nextgen-rushing_2020.csv.gz
│   │   ├── nextgen-rushing_2021.csv.gz
│   │   └── nextgen-rushing_2022.csv.gz
│   ├── pbp
│   │   ├── pbp_2016.parquet
│   │   ├── pbp_2017.parquet
│   │   ├── pbp_2018.parquet
│   │   ├── pbp_2019.parquet
│   │   ├── pbp_2020.parquet
│   │   ├── pbp_2021.parquet
│   │   └── pbp_2022.parquet
│   ├── pbp-participation
│   │   ├── pbp-participation_2016.parquet
│   │   ├── pbp-participation_2017.parquet
│   │   ├── pbp-participation_2018.parquet
│   │   ├── pbp-participation_2019.parquet
│   │   ├── pbp-participation_2020.parquet
│   │   ├── pbp-participation_2021.parquet
│   │   └── pbp-participation_2022.parquet
│   ├── player-stats
│   │   └── player-stats.parquet
│   └── players
│       └── players.parquet
├── requirements.txt
├── sandbox.ipynb
├── schemas
│   ├── advstats_season.pkl
│   ├── injuries.pkl
│   ├── nextgen_passing.pkl
│   ├── nextgen_receiving.pkl
│   ├── nextgen_rushing.pkl
│   ├── pbp.pkl
│   ├── pbp_participation.pkl
│   ├── player_stats.pkl
│   └── players.pkl
├── sql
│   └── wip.sql
└── src
├── __init__.py
├── __pycache__
│   ├── __init__.cpython-39.pyc
│   ├── build_power_scores.cpython-39.pyc
│   ├── configs.cpython-39.pyc
│   ├── database_loader.cpython-39-pytest-7.3.1.pyc
│   ├── database_loader.cpython-39.pyc
│   ├── db_utils.cpython-39.pyc
│   ├── inline_validation.cpython-39.pyc
│   ├── nflverse_loader_job.cpython-39.pyc
│   ├── nflverse_reader_job.cpython-39.pyc
│   ├── nflverse_transform_job.cpython-39.pyc
│   ├── pbp_fact.cpython-39.pyc
│   ├── pbp_participation.cpython-39.pyc
│   ├── player_injuries.cpython-39.pyc
│   ├── player_stats.cpython-39.pyc
│   ├── util_keras.cpython-39.pyc
│   ├── utils.cpython-39.pyc
│   └── utils_eda.cpython-39.pyc
├── build_power_scores.py
├── configs.py
├── database_loader.py
├── db_utils.py
├── inline_validation.py
├── nflverse_loader_job.py
├── nflverse_reader_job.py
├── nflverse_transform_job.py
├── pbp_fact.py
├── pbp_participation.py
├── player_injuries.py
├── player_stats.py
├── poc.py
├── temp.py
├── util
├── util_keras.py
├── util_torch.py
├── utils.py
└── utils_eda.py
