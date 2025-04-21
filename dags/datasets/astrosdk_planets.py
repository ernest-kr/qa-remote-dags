from datetime import datetime

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, Param
from astro.constants import FileType
from astro.files import File
from astro.sql import load_file, run_raw_sql, transform
from astro.sql.table import Table
from datasets.setup_planets import conn_id, csv_path

planet_data = File(path=str(csv_path), filetype=FileType.CSV)
mass = Table(name="planetary_mass", conn_id=conn_id)
mass_rank = Table(name="planetary_mass_rank", conn_id=conn_id)


@run_raw_sql(
    outlets=[mass],
)
def add_one(table: Table, name, mass):
    return """
    INSERT INTO {{table}}
    VALUES ({{name}}, {{mass}});
    """


@transform
def rank(input_table: Table):
    return """
    SELECT planet, RANK() OVER (ORDER BY mass DESC)
    FROM {{input_table}};
    """


@task
def announce_champion(rank_table: Table):
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    result = pg_hook.get_records(sql=f"SELECT planet FROM {rank_table.name} WHERE rank = '1';")
    print(result[0], "is the most massive planet")


with DAG(
    dag_id="astrosdk_initialize_known_masses",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["astro_sdk"],
    doc_md="Initialize the database with the planets that we already know about",
) as init:
    # planet,mass
    # MERCURY,0.330
    # VENUS,4.87
    # EARTH,5.97
    # MARS,0.642
    # JUPITER,1898
    # SATURN,568
    # URANUS,86.8
    # NEPTUNE,102

    load_file(
        task_id="initialize_with_known_planets",
        input_file=planet_data,
        output_table=mass,
        if_exists="replace",
    )


with DAG(
    dag_id="astrosdk_discover_planet",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    tags=["astro_sdk"],
    doc_md="Use DAG Params to input name and mass of newly discovered planet",
    params={
        "name": Param(
            "new_planet",
            type="string",
        ),
        "mass": Param(0, type="integer"),
    },
) as discover:
    add_one(
        table=mass,
        name='{{params["name"]}}',
        mass='{{params["mass"]}}',
    )

with DAG(
    dag_id="astrosdk_compare_masses",
    start_date=datetime(1970, 1, 1),
    schedule=[mass],
    tags=["astro_sdk"],
    doc_md="which planet is biggest?",
) as compare:
    rank(input_table=mass, output_table=mass_rank) >> announce_champion(mass_rank)
