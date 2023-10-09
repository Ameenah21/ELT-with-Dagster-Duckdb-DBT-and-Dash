from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource
from dagster import Definitions
import os

current_directory = os.getcwd()
database_file = os.path.join(current_directory, "my_duckdb_database.duckdb")


from . import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(
            database=database_file,
        )
    },
)
#print(all_assets)