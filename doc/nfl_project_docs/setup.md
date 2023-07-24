<div style="background-color: teal; padding: 5px; margin-top: 2px; margin-bottom: 2px;">
    <h3 style="color: white;">Project Structure</h3>
</div>

< [Back](main.md)

The project is structured as follows:

- source for EDA jobs in the root of the src/ directory:

      These run autonomously, but can also be run manually from the notebooks/nfl_load_nflverse_data_demo.ipynb notebook

  - nfl_00_load_nflverse_data.py   - downloads the nflverse data into local storage
  - nfl_01_build_nfl_database.py   - builds the nfl database from the nflverse data
  - nfl_02_prepare_weekly_stats.py - merges metrics from all datasets into a single stats dataset
  - nfl_03_perform_feature_selection.py - performs feature selection on the nfl data
  - nfl_04_merge_game_features.py - merge our features with the core nfl play actions dataset

  - nfl_main.py - orchestrates the jobs above
  - config.py - configuration file for the project

<br> 

- 3 notebooks in the root of the notebooks/ directory:

  - nfl_load_nflverse_data_demo.ipynb - demos manually running the load and build jobs (nfl_00 - 01)
  - nfl_perform_feature_selection_demo.ipynb - demos manually running weekly stats and feature selection jobs (nfl_02 - 04)
  - nfl_win_loss_classification_experiment2.ipynb - experiment 2 notebook

<br>

- ML models in the root of the models/ directory
<br>

- documentation in the doc/ directory
<br>

- schemas in the schemas/ directory - used during loading to validate the incoming data from nflverse

<br>


<img src="project_files.png" alt="Project Files" width="1000">
