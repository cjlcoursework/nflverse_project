# Ideas borrowed from @cooperdff
# Original code: https://github.com/cooperdff/nfl_data_py.git
# Modified by: Chris Lomeli
#

import os
import warnings


from NFLVersReader.src import database_loader

warnings.filterwarnings('ignore')

import pandas as pd
import requests

import pathlib

extention_funcs = {
    'csv': pd.read_csv,
    'csv.gz': pd.read_csv,
    'parquet': pd.read_parquet
}


def get_table_name(file_path):
    file_name = os.path.basename(file_path)
    return os.path.splitext(file_name)[0]


def load_file_as_pandas(file_path, loader):
    func_name = None
    for x in extention_funcs:
        if str(x).lower().endswith(x):
            func_name = x
            break
    if func_name is None:
        print(f"unsupported file type: {file_path}")
        return
    func = extention_funcs[func_name]

    handle_exists='replace'
    i=0
    for chunk in func(file_path, chunksize=1000):
        print(f"\tchunk {i}")
        i += 1
        loader.load_table(chunk, get_table_name(file_path), handle_exists=handle_exists)
        handle_exists = 'append'


def load_file_pyspark_pandas(file_name, loader):
    func_name = None
    for x in extention_funcs:
        if str(x).lower().endswith(x):
            func_name = x
            break
    if func_name is None:
        print(f"unsupported file type: {file_name}")
        return
    func = extention_funcs[func_name]

    df = func(file_name)
    table_name = get_table_name(file_name)
    loader.load_table(df, table_name)
    print(f"file={file_name}, shape={df.shape} : table={table_name}")


def convert_all_files_in_path(root_directory, match_list=None):
    dbloader = database_loader.DatabaseLoader("postgresql://postgres:chinois1@localhost/postgres")
    dbloader.connect_sql()

    for root, dirs, files in os.walk(root_directory):
        # Convert all files in the current directory
        for file_name in files:
            if match_list is not None:
                if file_name not in match_list:
                    continue
            source_file_path = os.path.join(root, file_name)
            print(source_file_path)
            load_file_as_pandas(source_file_path, dbloader)


if __name__ == '__main__':
    convert_all_files_in_path("../output", ['playbyplay_2021.csv'])
