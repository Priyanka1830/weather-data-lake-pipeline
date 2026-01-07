#  Automated Weather Data Lake Pipeline

A fault-tolerant, containerized ETL pipeline that ingests real-time weather data into an **AWS S3 Data Lake**. Orchestrated with **Apache Airflow** and Docker, this project enforces strict **Data Quality Gates** and optimizes storage using the **Parquet** format.

---

##  Architecture
**Flow:** `OpenWeatherMap API` $\rightarrow$ `Airflow (Extract)` $\rightarrow$ `Pandas (Validate & Transform)` $\rightarrow$ `AWS S3 (Load Parquet)`

* **Extract:** Fetches real-time weather data for Bangalore via API.
* **Transform:** Validates schema, checks for nulls/outliers, and formats data.
* **Load:** Saves highly optimized `.parquet` files to an S3 Data Lake.

---

##  Airflow UI Screenshots

### 1. Airflow DAG (Extract $\rightarrow$ Transform $\rightarrow$ Load)
*Structure of DAG & example of an execution.*
![DAG Execution](screen_shots\DAG_image.png)

### 2. XCom being utilized
*Data being passed from one node to another via XCom.*
![Xcom from 1st node](screen_shots\XCom_1.png)
![Xcom from 2nd node](screen_shots\XCom_2.png)

### 3. Execution Logs
*Execution logs of the Data Quality check & validation logic.*
![Validation logs](screen_shots\Validation_logs.png)

### 4. AWS S3 Data Lake
*Parquet files being written to the S3 path.*
![S3 uploads](screen_shots\Data_loaded_to_S3.png)


---

##  Tech Stack
* **Orchestration:** Apache Airflow
* **Containerization:** Docker & Docker Compose
* **Cloud Storage:** AWS S3 (Simple Storage Service)
* **Language:** Python 3.9+ (Pandas, Boto3)
* **Format:** Apache Parquet (Columnar Storage)

---

##  How to Run Locally

### 1. Prerequisites

* Docker Desktop installed and running.
* An [OpenWeatherMap API Key](https://openweathermap.org/api) (Free).
* AWS Access Keys with `AmazonS3FullAccess`.

### 2. Clone the Repository

```bash
git clone git@github.com:Priyanka1830/weather-data-lake-pipeline.git
cd weather-data-lake-pipeline
```

### 3. Configure Environment Variables

Create a .env file in the root directory:
```toml
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=us-east-1
OPEN_WEATHER_API_KEY=your_weather_api_key
AIRFLOW_UID=50000
```

### 4. Initialize the database and the user

```bash
docker-compose run --rm airflow-webserver airflow db migrate
docker-compose run --rm airflow-webserver airflow users create --username admin --firstname John --lastname Doe --role Admin --email john.doe@example.com --password admin
```

### 5. Start the Pipeline

```bash
docker-compose up -d --build
```

- Access Airflow UI: http://localhost:8080

- Login: admin / admin

- Trigger the weather_data_lake_pipeline DAG.
