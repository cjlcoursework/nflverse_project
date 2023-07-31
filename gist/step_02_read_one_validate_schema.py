# ----------------------------
import os

import requests


def read_one_source(url: str, output_directory: str, output_file_name: str , force: bool = True):
    """
    Download the file from the url and save it locally under `file_name`
    """

    # make sure the directory exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # construct the full path to the new file
    full_path = os.path.join(output_directory, output_file_name)

    # if the file exists and force is False, return 400
    if os.path.exists(full_path):
        if force:
            os.remove(full_path)
        else:
            return 400

    # get the file from the url
    response = requests.get(url)

    # if the response is 200, write the file
    if response.status_code == 200:
        with open(full_path, "wb") as file:
            file.write(response.content)

    return response.status_code


def test_readon_one_source_test():
    pbp_url = 'https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{year}.{file_type}'
    pbp_url = pbp_url.format(year=2019, file_type='parquet')
    directory = './data/pbp'
    file_name = 'play_by_play_2019.parquet'

    read_one_source(pbp_url, directory, file_name)
