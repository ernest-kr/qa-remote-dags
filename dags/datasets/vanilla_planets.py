from datetime import datetime

from airflow.datasets import Dataset
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sdk import DAG, Param
from datasets.setup_planets import conn_id, csv_path

mass = Dataset("planetary_mass")


@task
def announce_champion():
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    result = pg_hook.get_records(sql="SELECT planet FROM planetary_mass_rank WHERE rank = '1';")
    print(result[0], "is the most massive planet")


with DAG(
    dag_id="vanilla_initialize_known_masses",
    start_date=datetime(1970, 1, 1),
    schedule=None,
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

    @init.task
    def build_sql():
        values = []
        with open(csv_path) as f:
            for line in f.readlines():
                planet, mass = line.split(",")
                if planet != "planet":
                    values.append((planet, mass))

        sql = """
        DROP TABLE IF EXISTS planetary_mass;
        CREATE TABLE planetary_mass(planet varchar, mass numeric);
        INSERT INTO planetary_mass VALUES {};
        """.format(
            ",".join([f"('{planet}', '{mass}')" for (planet, mass) in values])
        )
        return sql

    PostgresOperator(task_id="insert_rows", sql=build_sql(), postgres_conn_id=conn_id, outlets=[mass])


with DAG(
    dag_id="vanilla_discover_planet",
    start_date=datetime(1970, 1, 1),
    schedule=None,
    doc_md="Use DAG Params to input name and mass of newly discovered planet",
    params={
        "name": Param(
            "new_planet",
            type="string",
        ),
        "mass": Param(0, type="integer"),
    },
) as discover:
    PostgresOperator(
        task_id="add_one",
        postgres_conn_id=conn_id,
        sql="""
            INSERT INTO planetary_mass
            VALUES ('{{ params["name"] }}', '{{ params["mass"] }}');
            """,
        outlets=[mass],
    )


with DAG(
    dag_id="vanilla_compare_masses",
    start_date=datetime(1970, 1, 1),
    schedule=[mass],
    doc_md="which planet is biggest?",
) as compare:

    @compare.task
    def announce_champion():
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        result = pg_hook.get_records(sql="SELECT planet FROM planetary_mass_rank WHERE rank = 1;")
        print(result[0], "is the most massive planet")

    (
        PostgresOperator(
            task_id="rank_them",
            postgres_conn_id=conn_id,
            sql="""
            DROP TABLE IF EXISTS planetary_mass_rank;

            CREATE TABLE planetary_mass_rank AS
            SELECT planet, RANK() OVER (ORDER BY mass DESC)
            FROM planetary_mass;
            """,
        )
        >> announce_champion()
    )
