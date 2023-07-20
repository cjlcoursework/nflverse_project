import logging
import os.path
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import requests

from src import *
from src.util.utils import assert_and_alert

# Configure logging
logger = configure_logging("pbp_logger")


def validate_schema(local_file_path, schema_file, silent=True):
    schema_df = pd.read_csv(schema_file, header=0)

    df = pd.read_csv(local_file_path, low_memory=False)
    df_dtypes = pd.DataFrame(df.dtypes, columns=['type']).reset_index().rename(columns={"index": "column"})
    assert_and_alert(
        df_dtypes.equals(schema_df),
        msg=f"Schemas do not match:  {schema_file} --> {local_file_path}",
        silent=silent
    )


def read_source(url, output_dir, local_file_base, schema_file_path=None, silent=True):
    # Make the HTTP request to get the Parquet file content

    base_file_name = os.path.basename(url)

    local_file_base_name = local_file_base + base_file_name[base_file_name.index('.'):]
    if "_" not in local_file_base_name:
        dir_path = os.path.join(output_dir, local_file_base)
    else:
        dir_path = os.path.join(output_dir, *local_file_base_name.split("_")[:-1])

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    full_path = os.path.join(dir_path, local_file_base_name)

    # Check if the request was successful (status code 200)
    response = requests.get(url)
    if response.status_code == 200:

        # Write to the local_file_path
        with open(full_path, "wb") as file:
            file.write(response.content)
            logger.info(f"Success: {url}")

        # validate against the schema if one was sent in
        if schema_file_path is not None:
            validate_schema(output_dir, schema_file_path, silent)

    else:
        logger.info(f"Failed  : {url}")
    return response.status_code


class URLReader:

    def __init__(self, start_year: int, last_year: int, file_type='csv', max_workers=10):
        self.output_directory = get_config('output_directory')
        self.schema_directory = get_config('schema_directory')
        self.file_type = file_type
        self.years = np.arange(start_year, last_year + 1)
        self.max_workers = max_workers

    def get_urls(self):
        advstats_stat_types = get_config('advstats_stat_types')
        ng_stat_types = get_config('ng_stats_types')

        pbp_url_template = get_config('pbp_url')
        pbp_participation_url_template = get_config('pbp_participation_url')
        injuries_url_template = get_config('injuries_url')
        player_stats_url_template = get_config('player_stats_url')
        advstats_url_template = get_config('advstats_url')
        players_url_template = get_config('players_url')
        ng_stats_url_template = get_config('ng_stats_url')

        urls = {
            'player-stats': player_stats_url_template.format(file_type=self.file_type),
            'players': players_url_template.format(file_type=self.file_type)
        }

        for year in self.years:
            urls[f"pbp_{year}"] = pbp_url_template.format(year=year, file_type=self.file_type)
            urls[f"pbp-participation_{year}"] = pbp_participation_url_template.format(year=year,
                                                                                      file_type=self.file_type)
            urls[f"injuries_{year}"] = injuries_url_template.format(year=year, file_type=self.file_type)
            for stat in ng_stat_types:
                urls[f"nextgen-{stat}_{year}"] = ng_stats_url_template.format(year=year, stat_type=stat)

        for stat in advstats_stat_types:
            urls[f"advstats-season-{stat}"] = advstats_url_template.format(stats_type=stat, file_type=self.file_type)

        return urls

    def download(self):
        urls = self.get_urls()

        test_only = False

        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

        # Submit download tasks to the executor
        if test_only:
            [read_source(url, self.output_directory, file_name) for file_name, url in urls.items()]
        else:
            executor = ThreadPoolExecutor(max_workers=self.max_workers)
            tasks = [
                executor.submit(read_source, url, self.output_directory, file_name )
                for file_name, url in urls.items()
            ]

            # Wait for all tasks to complete
            for future in as_completed(tasks):
                try:
                    result = future.result()
                    # print(f'Successfully downloaded: {result}')
                except Exception as e:
                    logger.info(f'Error occurred: {str(e)}')

        return urls


def read_nflverse_datasets():
    logger.setLevel(logging.DEBUG)
    reader = URLReader(start_year=2016, last_year=2022, file_type=get_config("file_type"))
    urls = reader.download()


if __name__ == '__main__':
    read_nflverse_datasets()