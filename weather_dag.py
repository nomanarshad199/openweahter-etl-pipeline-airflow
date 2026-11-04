
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import boto3
from io import StringIO


def kelvin_to_fahrenheit(kelvin_temp):
    return (kelvin_temp - 273.15) * 9 / 5 + 32


def extract_load_data(task_instance):

    data = task_instance.xcom_pull(task_ids='extract_weather_data')

    city = data['name']
    weather_description = data['weather'][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp'])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data['main']['feels_like'])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_min'])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data['main']['temp_max'])
    pressure = data['main']['pressure']
    wind_speed = data['wind']['speed']
    humidity = data['main']['humidity']

    time_of_record = datetime.utcfromtimestamp(
        data['dt'] + data['timezone']
    ).strftime('%Y-%m-%d %H:%M:%S')

    sunrise_time = datetime.utcfromtimestamp(
        data['sys']['sunrise'] + data['timezone']
    ).strftime('%Y-%m-%d %H:%M:%S')

    sunset_time = datetime.utcfromtimestamp(
        data['sys']['sunset'] + data['timezone']
    ).strftime('%Y-%m-%d %H:%M:%S')

    # Create transformed dataset
    transformed_data = {
        'City': city,
        'Description': weather_description,
        'Temperature (F)': temp_fahrenheit,
        'Feels Like (F)': feels_like_fahrenheit,
        'Minimum Temp (F)': min_temp_fahrenheit,
        'Maximum Temp (F)': max_temp_fahrenheit,
        'Pressure': pressure,
        'Humidity': humidity,
        'Wind Speed': wind_speed,
        'Time of Record': time_of_record,
        'Sunrise (Local Time)': sunrise_time,
        'Sunset (Local Time)': sunset_time
    }

    transformed_data_list = [transformed_data]
    df = pd.DataFrame(transformed_data_list)

    # Generate filename
    now = datetime.now()
    dt_string = now.strftime('%Y-%m-%d_%H-%M-%S')

    # S3 bucket details
    bucket_name = 'weatherapi-airfloww'
    s3_key = f'weather_data/weather_data_{dt_string}.csv'
    s3_path = f's3://{bucket_name}/{s3_key}'

    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload CSV to S3 using boto3
    s3_client = boto3.client('s3')

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    print(f'Successfully uploaded weather data to {s3_path}')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 28),
    'email': ['arshadnoman659@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


with DAG(
    'weather_dag',
    default_args=default_args,
    description='A simple weather DAG',
    schedule='@daily',
    catchup=False,
) as dag:

    is_weather_data_available = HttpSensor(
        task_id='is_weather_data_available',
        http_conn_id='weather_api_conn',
        endpoint='weather',
        request_params={
            'q': 'Lahore',
            'appid': '{{ conn.weather_api_conn.password }}',
        },
    )

    extract_weather_data = HttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weather_api_conn',
        endpoint='weather',
        method='GET',
        data={
            'q': 'Lahore',
            'appid': '{{ conn.weather_api_conn.password }}',
        },
        response_check=lambda response: response.status_code == 200,
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    extract_load_weather_data = PythonOperator(
        task_id='extract_load_weather_data',
        python_callable=extract_load_data,
    )

    is_weather_data_available >> extract_weather_data >> extract_load_weather_data
