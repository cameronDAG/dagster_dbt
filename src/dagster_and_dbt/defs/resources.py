from dagster_dbt import DbtCliResource
from dagster import Definitions
from dagster_duckdb import DuckDBResource
from dagster import EnvVar



from dagster_and_dbt.defs.project import dbt_project
# the import lines go at the top of the file

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")      # replaced with environment variable
)
# this can be defined anywhere below the imports
dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

defs = Definitions(
    resources={
        "database": database_resource,
        "dbt": dbt_resource,
    },
)
