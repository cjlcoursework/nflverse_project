
<div style="background-color: teal; padding: 10px;">
    <h3 style="color: white;">Workflow</h3>
</div>

< [Back](main.md)
<br>


![NFL Workflow](nfl.png)
<br>

The jobs are meant to run autonomously, but for demo purposes we can manually run the ETL and feature selection jobs from the notebooks/nfl_load_nflverse_data_demo.ipynb notebook.
- <font color=teal>**nfl_load_nflverse_data_demo.ipynb**</font> - demos manually running the load and build jobs (nfl_00 - 01)
- <font color=teal>**nfl_perform_feature_selection_demo.ipynb**</font> - demos manually running weekly stats and feature selection jobs (nfl_02 - 04)

| Job                                                         | Stage         | Description                                                  | Outcome                                |
|-------------------------------------------------------------|---------------|--------------------------------------------------------------|----------------------------------------|
| <font color=teal>nfl_main.py</font>                         | orchestration | runs the ETL and feature selection for the win/loss model    | runs everything                        |
| <font color=teal>nfl_00_load_nflverse_data.py</font>        | ETL           | downloads the nflverse data into local storage               | files loaded into the data directory   |
| <font color=teal>nfl_01_build_nfl_database.py</font>        | ETL           | builds the nfl database from the nflverse data               | database schema and tables             |
| <font color=teal>nfl_02_prepare_weekly_stats.py</font>      | Experiment 2  | merges metrics from all datasets into a single stats dataset | temp_weekly .. parquet files           |
| <font color=teal>nfl_03_perform_feature_selection.py</font> | Experiment 2  | performs feature selection on the nfl data                   | tmp_**_week_features files             |
| <font color=teal>nfl_04_merge_game_features.py</font>       | Experiment 2  | merge our features with the core nfl play actions dataset    | weekly_game_stats used by experiment 2 |


<br>

Finally, we can run the experiment 2 notebook <font color=teal>**nfl_win_loss_classification_experiment2.ipynb**</font> - experiment 2 notebook

