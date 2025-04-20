from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests
from datetime import datetime
from psycopg2.extras import execute_values
import pytz

# --- Weather API Info ---
API_KEY = "xxxx"
CITY = "Paris"
UNITS = "metric"

# --- PostgreSQL Connection Info ---
host = "localhost"
port = 5432
database = "postgres"
user = "xxxx"
password = "xxxx"

# Define DAG and its schedule
dag = DAG(
    'weather_data_pipeline',
    description='Fetch, Transform, and Load Weather Data',
    schedule_interval='@daily',  # Runs once a day, adjust if needed
    start_date=datetime(2025, 4, 17),
    catchup=False
)

# --- Task 1: Get Data from API ---
def get_weather_data():
    weather_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": CITY,
        "appid": API_KEY,
        "units": UNITS
    }
    response = requests.get(weather_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        # Return the raw data as a dictionary
        return data
    else:
        raise Exception(f"Error {response.status_code}: {response.text}")

# --- Task 2: Transform the Data ---
def transform_data(ti):
    # Pull the raw weather data from Task 1
    raw_data = ti.xcom_pull(task_ids='get_weather_data')
    
    if not raw_data:
        raise Exception("No data retrieved from the API.")
    
    # Extract the city and its timezone information
    city_name = raw_data["name"]
    country_code = raw_data["sys"]["country"]
    city_timezone = raw_data["timezone"]  # This gives the timezone offset in seconds from UTC

    # Convert the Unix timestamp to UTC
    utc_time = datetime.utcfromtimestamp(raw_data["dt"])

    # Convert the UTC time to the city's local time using pytz
    local_tz = pytz.FixedOffset(city_timezone // 60)  # Convert from seconds to minutes
    local_time = utc_time.replace(tzinfo=pytz.utc).astimezone(local_tz)

    # Transform the raw data into the desired format
    weather_record = {
        "city": city_name,
        "country": country_code,
        "timestamp": local_time,  # Use local_time instead of UTC time
        "weather": raw_data["weather"][0]["description"],
        "temp": raw_data["main"]["temp"],
        "temp_min": raw_data["main"]["temp_min"],
        "temp_max": raw_data["main"]["temp_max"],
        "humidity": raw_data["main"]["humidity"],
        "pressure": raw_data["main"]["pressure"],
        "wind_speed": raw_data["wind"]["speed"]
    }
    
    return weather_record

# --- Task 3: Load Data into PostgreSQL ---
def load_to_postgresql(ti):
    # Pull the transformed data from Task 2
    transformed_data_list = ti.xcom_pull(task_ids='transform_data')

    if not transformed_data_list:
        raise Exception("No transformed data to insert into PostgreSQL.")
    
    try:
        # Connect to PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')  # Use Airflow connection
        # If not using Airflow's connection UI, use this:
        # postgres_hook = PostgresHook(host="localhost", port="5433", database="postgres", user="postgres", password="Wow@123456")

        # --- Create Table if not exists ---
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city TEXT,
            country TEXT,
            timestamp TIMESTAMP,
            weather TEXT,
            temp REAL,
            temp_min REAL,
            temp_max REAL,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed REAL,
            UNIQUE(city, country, timestamp)  -- Ensure no duplicates based on city, country, and timestamp
        );
        """
        
        # Run create table query
        postgres_hook.run(create_table_query)

        # --- Insert Data (Batch Insert) ---
        insert_query = """
        INSERT INTO weather_data (
            city, country, timestamp, weather, temp, temp_min, temp_max,
            humidity, pressure, wind_speed
        ) VALUES %s
        ON CONFLICT (city, country, timestamp) DO NOTHING;  -- Skip duplicates
        """
        
        # Prepare values to insert
        values = [
            (
                data["city"],
                data["country"],
                data["timestamp"],
                data["weather"],
                data["temp"],
                data["temp_min"],
                data["temp_max"],
                data["humidity"],
                data["pressure"],
                data["wind_speed"]
            )
            for data in transformed_data_list
        ]
        
        # Connect to the database and execute the insertion
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Execute the batch insert query
        execute_values(cursor, insert_query, values)
        
        # Commit the transaction
        conn.commit()

        print("✅ Weather data inserted successfully!")

    except Exception as e:
        # Log the error and raise the exception
        print(f"Error occurred while loading data to PostgreSQL: {e}")
        raise  # Re-raise the exception to mark the task as failed in Airflow

    finally:
        # Ensure that the connection is closed, even if an error occurs
        if conn:
            cursor.close()
            conn.close()

# --- Define Airflow Tasks ---
task_1 = PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather_data,
    dag=dag
)

task_2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Passes context variables to the function (e.g., XCom)
    dag=dag
)

task_3 = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    provide_context=True,  # Passes context variables to the function (e.g., XCom)
    dag=dag
)

# --- Set Task Dependencies ---
task_1 >> task_2 >> task_3  # Task 1 -> Task 2 -> Task 3
