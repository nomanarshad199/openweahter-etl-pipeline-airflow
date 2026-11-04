# Weather ETL Pipeline Using Apache Airflow and AWS

## Project Overview

This project implements an automated **ETL (Extract, Transform, Load) pipeline** that retrieves real-time weather data from the OpenWeatherMap API, transforms it using Python and Pandas, and stores the resulting CSV file in Amazon S3.

The pipeline is orchestrated using Apache Airflow and runs on an AWS EC2 Ubuntu instance.

The project demonstrates practical skills in API integration, data extraction, transformation, workflow orchestration, cloud infrastructure, and object storage.

## Architecture

```text
OpenWeatherMap API
        |
        v
Apache Airflow (AWS EC2)
        |
        v
Check API Availability
      (HttpSensor)
        |
        v
Extract Weather Data
     (HttpOperator)
        |
        v
Transform Weather Data
  (Python + Pandas)
        |
        v
Generate CSV File
        |
        v
Amazon S3
```

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | ETL logic and data processing |
| Apache Airflow | Workflow orchestration and scheduling |
| Pandas | Data transformation and CSV generation |
| OpenWeatherMap API | Source of weather data |
| AWS EC2 | Hosting the Airflow environment |
| Amazon S3 | Storing transformed CSV files |
| Boto3 | Connecting Python to AWS S3 |
| Ubuntu | EC2 operating system |
| Git & GitHub | Version control and project hosting |

## ETL Workflow

The Airflow DAG contains three main tasks:

### 1. Check API Availability — `HttpSensor`

- Checks whether the OpenWeatherMap API is accessible.
- Sends a request to the weather endpoint.
- Allows the pipeline to proceed when the API responds successfully.

### 2. Extract Weather Data — `HttpOperator`

- Sends a GET request to the OpenWeatherMap API.
- Retrieves weather information for Lahore.
- Checks the HTTP response status.
- Extracts the JSON response and passes it to the next task through Airflow XCom.

### 3. Transform and Load Data — `PythonOperator`

- Retrieves the extracted JSON data from XCom.
- Extracts relevant weather attributes.
- Converts temperatures from Kelvin to Fahrenheit.
- Processes timestamps using the city's timezone offset.
- Creates a Pandas DataFrame.
- Converts the transformed data into CSV format.
- Uploads the CSV file to Amazon S3 using Boto3.

## Data Transformation

The pipeline extracts and transforms the following information:

| Column | Description |
|---|---|
| City | City name |
| Description | Weather conditions |
| Temperature (F) | Current temperature in Fahrenheit |
| Feels Like (F) | Perceived temperature |
| Minimum Temp (F) | Minimum temperature |
| Maximum Temp (F) | Maximum temperature |
| Pressure | Atmospheric pressure |
| Humidity | Humidity percentage |
| Wind Speed | Wind speed |
| Time of Record | Local weather record time |
| Sunrise (Local Time) | Local sunrise time |
| Sunset (Local Time) | Local sunset time |

## AWS Infrastructure

The project uses the following AWS services:

**Amazon EC2**
- Hosts the Ubuntu environment.
- Runs Apache Airflow and its supporting Python dependencies.
- Executes the scheduled ETL workflow.

**Amazon S3**
- Stores the transformed weather data as CSV files.
- Each execution generates a timestamped filename.
- Files are stored under the `weather_data/` prefix.

**AWS IAM Role**
- An IAM role is attached to the EC2 instance.
- Boto3 uses the instance role to authenticate with AWS.
- This avoids hardcoding AWS access keys in the Python code.

## Airflow Configuration

- DAG ID: `weather_dag`
- Schedule: Daily (`@daily`)
- Catchup: Disabled
- Retries: 2
- Retry delay: 2 minutes
- API connection ID: `weather_api_conn`

The API key is stored in the Airflow connection rather than hardcoded in the DAG.

## Project Structure

```text
openweahter-etl-pipeline-airflow/
│
├── weather_dag.py
├── .gitignore
└── README.md
```

## Output

The pipeline generates a CSV file containing the transformed weather data and uploads it to:

```text
s3://weatherapi-airfloww/weather_data/
```

Example filename:

```text
weather_data_YYYY-MM-DD_HH-MM-SS.csv
```

Each successful execution creates a new timestamped CSV file.

## Screenshots



### Airflow DAG

<img width="977" height="435" alt="image" src="https://github.com/user-attachments/assets/05372f95-f6b8-4de7-b07a-b0f04a16e8e0" />


### Successful DAG Execution

<img width="908" height="289" alt="image" src="https://github.com/user-attachments/assets/69f3092f-cfbc-48f8-b606-ab888973534a" />


### Amazon S3 Output

<img width="1334" height="400" alt="image" src="https://github.com/user-attachments/assets/472445d3-ae1d-4fcd-8da6-0a923b373e5d" />


### Transformed CSV Data

<img width="960" height="41" alt="image" src="https://github.com/user-attachments/assets/26a619d6-8a87-4cf2-ad4f-6b33c7d8ec15" />


## Key Learning Outcomes

Through this project, I gained practical experience with:

- Building an API-based ETL pipeline.
- Working with JSON API responses.
- Transforming data using Python and Pandas.
- Orchestrating tasks using Apache Airflow.
- Understanding task dependencies and XCom.
- Deploying Airflow on an AWS EC2 instance.
- Uploading data to Amazon S3 using Boto3.
- Managing AWS permissions through IAM roles.
- Using Git and GitHub for version control.

## Future Improvements

- Store the original API JSON response separately in S3.
- Organize S3 data into raw and processed layers.
- Add data validation and error-handling checks.
- Add monitoring and logging improvements.
- Extend the pipeline to collect weather data from multiple cities.
- Build a dashboard to visualize historical weather data.
