# Ideas borrowed from @cooperdff
# Original code: https://github.com/cooperdff/nfl_data_py.git
# Modified by: Chris Lomeli
#
import gzip
import os
import pathlib
import pickle
import warnings

from src import *

logger = configure_logging("pbp_logger")

warnings.filterwarnings('ignore')


def uncompress_gzip(input_file, output_file):
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            f_out.write(f_in.read())


def compare_schema(current_dict: dict, new_dict: dict):
    logger.info("------------- compare-----------------")
    for key in new_dict:
        if key not in current_dict:
            logger.info("Missing key : ", key)
        elif new_dict[key] != current_dict[key]:
            logger.info(f"Mismatch {key}  ::  new key: {new_dict[key]} != original: {current_dict[key]}")


def write_schema(df: pd.DataFrame, name: str):
    write_schema_file(name, df.dtypes.to_dict())


def write_schema_file(name, schema, force=False):
    directory = get_config("schema_directory")
    file_name = f'{directory}/{name}.pkl'
    if os.path.exists(file_name) and not force:
        return
    with open(file_name, 'wb') as file:
        logger.info(f"Writing file {file_name}")
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

    dtypes = read_schema(table_name)

    func = get_read_function(file_path)

    df = func(file_path, dtype=dtypes)
    logger.info(f"""table_name={table_name}, {len(df)} records... file={os.path.basename(file_path)}, schema={schema}, handle_exists={handle_exists}""")

    # write_schema(df, table_name)
    for attempt in range(2):
        try:
            loader.load_table(
                df=df,
                table_name=table_name,
                schema=schema,
                handle_exists=handle_exists, source=file_path)
            break
        except Exception as e:
            table_name = table_name + "_x"
            logger.info(f"Fix attempt: {attempt} table with errors : {table_name} <--{file_path}")
            handle_exists = "replace"


def bulk_load_csv_file(table_name: str, file_path: str, loader: DatabaseLoader, schema: str ='public', handle_exists: str ='replace'):
    test_only = False

    if file_path.endswith(".gz"):
        output_file = file_path.rsplit(".", maxsplit=1)
        uncompress_gzip(file_path, output_file)
        file_path = output_file

    logger.info(f"""table_name={table_name}, file={os.path.basename(file_path)}, schema={schema}, handle_exists={handle_exists}""")

    dtypes = read_schema(table_name)

    # write_schema(df, table_name)
    for attempt in range(2):
        try:
            loader.bunk_copy_table(
                file_path,
                table_name=table_name,
                dtypes=dtypes,
                schema=schema,
                handle_exists=handle_exists, source="nflverse")
            break
        except Exception as e:
            table_name = table_name + "_x"
            logger.info(f"Fix attempt: {attempt} table with errors : {table_name} <--{file_path}")
            handle_exists = "replace"


def convert_all_files_in_path(dbloader, root_directory, schema='public'):
    dbloader.connect_sql()
    bulk_load = False

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

            if bulk_load:
                bulk_load_csv_file(
                    table_name=table_name,
                    schema=schema,
                    file_path=source_file_path,
                    loader=dbloader, handle_exists=handle_exists)
            else:
                load_file_as_pandas(
                    table_name=table_name,
                    schema=schema,
                    file_path=source_file_path,
                    loader=dbloader, handle_exists=handle_exists)

    done.add(table_name)


# if __name__ == '__main__':
#     connection_string = get_config('connection_string')
#     files_directory = get_config('output_directory')
#
#     dbloader = database_loader.DatabaseLoader(connection_string)
#
#     convert_all_files_in_path(dbloader, files_directory, schema='controls')
#
#     logger.info(" --- Load stats ---")
#     for i in dbloader.load_stats:
#         logger.info(i)

