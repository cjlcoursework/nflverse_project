# Ideas borrowed from @cooperdff
# Original code: https://github.com/cooperdff/nfl_data_py.git
# Modified by: Chris Lomeli
#

import os
import pathlib
import pickle
import warnings

import pandas as pd

from NFLVersReader.src.nflverse_clean import database_loader
from NFLVersReader.src.nflverse_clean.configs import get_config

warnings.filterwarnings('ignore')

tested_extensions = {
    'csv': pd.read_csv,
    'csv.gz': pd.read_csv,
    'parquet': pd.read_parquet
}


def compare_schema(current_dict: dict, new_dict: dict):
    print("------------- compare-----------------")
    for key in new_dict:
        if key not in current_dict:
            print("Missing key : ", key)
        elif new_dict[key] != current_dict[key]:
            print(f"Mismatch {key}  ::  new key: {new_dict[key]} != original: {current_dict[key]}")


def write_schema(df: pd.DataFrame, name: str):
    write_schema_file(name, df.dtypes.to_dict())


def write_schema_file(name, schema, force=False):
    directory = get_config("schema_directory")
    file_name = f'{directory}/{name}.pkl'
    if os.path.exists(file_name) and not force:
        return
    with open(file_name, 'wb') as file:
        print(f"Writing file {file_name}")
        pickle.dump(schema, file)


def read_schema(name):
    directory = get_config("schema_directory")
    file_name = f'{directory}/{name}.pkl'
    if not os.path.exists(file_name):
        return None
    with open(file_name, 'rb') as file:
        dtypes = pickle.load(file)
        return dtypes


def load_file_as_pandas(table_name, file_path, loader, schema='public', handle_exists='replace'):
    test_only = False
    func_name = None
    for x in tested_extensions:
        if str(x).lower().endswith(x):
            func_name = x
            break
    if func_name is None:
        print(f"unsupported file type: {file_path}")
        return
    func = tested_extensions[func_name]

    dtypes = read_schema(table_name)

    i = 0
    print(f"""table_name={table_name}, file={file_path}, schema={schema}, handle_exists={handle_exists}""")

    df = func(file_path, dtype=dtypes)
    new_dtypes = df.dtypes.to_dict()

    # write_schema(df, table_name)
    for attempt in range(2):
        try:
            loader.load_table(
                df=df,
                table_name=table_name,
                schema=schema,
                handle_exists=handle_exists)
            break
        except Exception as e:
            table_name = table_name + "_x"
            print(f"Fix attempt: {attempt} table with errors : {table_name} <--{file_path}")
            handle_exists = "replace"


def convert_all_files_in_path(dbloader, root_directory, schema='public'):
    dbloader.connect_sql()

    done = set()

    for root, dirs, files in os.walk(root_directory):
        # Convert all files in the current directory

        for file_name in files:
            extension = pathlib.Path(file_name).suffix[1:]
            if extension not in tested_extensions:
                continue

            source_file_path = os.path.join(root, file_name)
            relative_path = source_file_path.replace(root_directory, "")

            table_name = relative_path.split('/')[1] \
                .replace("-", "_").split('.')[0]

            handle_exists = 'replace'
            if table_name in done:
                handle_exists = 'append'

            load_file_as_pandas(
                table_name=table_name,
                schema=schema,
                file_path=source_file_path,
                loader=dbloader, handle_exists=handle_exists)

            done.add(table_name)


if __name__ == '__main__':
    connection_string = get_config('connection_string')
    files_directory = get_config('output_directory')

    dbloader = database_loader.DatabaseLoader(connection_string)

    convert_all_files_in_path(dbloader, files_directory, schema='controls')
