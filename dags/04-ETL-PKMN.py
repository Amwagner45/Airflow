# imports important for Airflow
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

# Import Modules for code
import requests
import psycopg2


# [START instantiate_dag]
@dag(
    schedule_interval=None,  # interval how often the dag will run (can be cron expression as string)
    start_date=pendulum.datetime(
        2021, 1, 1, tz="UTC"
    ),  # from what point on the dag will run (will only be scheduled after this date)
    catchup=False,  # no catchup needed, because we are running an api that returns now values
    tags=["LearnDataEngineering"],  # tag the DAQ so it's easy to find in AirflowUI
)
def ETLPokemonPostgresAndPrint():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # EXTRACT: Query the data from the Pokemon
    @task()
    def extract(id):
        r = requests.get(f"https://pokeapi.co/api/v2/pokemon/{id}")

        # Get the json
        r_json = r.json()

        print(type(r_json))

        return r_json

    # TRANSFORM: Transform the API response into something that is useful for the load
    @task()
    def transform(r_json):
        """
        A simple Transform task which takes in the API data
        """

        print(r_json)

        data_dict = {
            "id": r_json["id"],
            "name": r_json["name"],
            "type1": r_json["types"][0]["type"]["name"],
            "type2": r_json["types"][1]["type"]["name"],
            "weight": r_json["weight"],
            "hp": int(r_json["stats"][0]["base_stat"]),
            "attack": int(r_json["stats"][1]["base_stat"]),
            "defense": int(r_json["stats"][2]["base_stat"]),
            "specialattack": int(r_json["stats"][3]["base_stat"]),
            "specialdefense": int(r_json["stats"][4]["base_stat"]),
            "speed": int(r_json["stats"][5]["base_stat"]),
        }

        return data_dict

    # LOAD: Save the data into Postgres database
    @task()
    def load(pokemon_data: dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to the posgres database.
        """

        try:
            connection = psycopg2.connect(
                user="airflow",
                password="airflow",
                host="postgres",
                port="5432",
                database="PokemonData",
            )
            cursor = connection.cursor()

            postgres_insert_query = """INSERT INTO pokemon (id, name, type1, type2, weight, hp, attack, defense, specialattack, specialdefense, speed) VALUES ( %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            record_to_insert = (
                pokemon_data["id"],
                pokemon_data["name"],
                pokemon_data["type1"],
                pokemon_data["type2"],
                pokemon_data["weight"],
                pokemon_data["hp"],
                pokemon_data["attack"],
                pokemon_data["defense"],
                pokemon_data["specialattack"],
                pokemon_data["specialdefense"],
                pokemon_data["speed"],
            )
            cursor.execute(postgres_insert_query, record_to_insert)

            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")

        except (Exception, psycopg2.Error) as error:
            print("Failed to insert record into mobile table", error)

            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

            raise Exception(error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    @task()
    def query_print(data: dict):
        """
        #### Print task
        This just prints out the result into the log (even without instantiating a logger)
        """
        print(data)

    @task()
    def sleep_time(duration):
        from time import sleep

        sleep(duration)

    # # List of pokemon IDs
    # poke_list = []
    # for i in range(1, 1017):
    #     poke_list.append(i)

    # for i in range(10000, 10275):
    #     poke_list.append(i)

    poke_list = [1, 2, 483, 484]

    # Define the main flow
    for i in poke_list:
        pokemon_data = extract(i)
        pokemon_summary = transform(pokemon_data)
        load(pokemon_summary)
        sleep_time(1)
    query_print(pokemon_summary)


lde_pokemon_dag_posgres = ETLPokemonPostgresAndPrint()
