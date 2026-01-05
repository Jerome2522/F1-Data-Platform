# F1 Data Engineering Platform

## 🏎️ Overview
This project is an end-to-end, Dockerized data platform that ingests, processes, and visualizes Formula 1 data.
It allows for analysis of Driver Standings, Constructor reliability, and Driver Consistency using the [Ergast F1 API](http://ergast.com/mrd/).

**Note:** This is a **Near-Real-Time (NRT)** system. It relies on batch processing triggers (Airflow DAGs) to refresh data and is not a streaming event-driven pipeline.

## 🏗️ Architecture
The platform is composed of 4 Docker services connected via a shared volume:
1.  **Airflow (`airflow`)**: Orchestrator. Triggers ingestion and processing jobs using `DockerOperator`.
2.  **Ingestion (`ingestion`)**: Ephemeral Job. Python script that fetches data from Ergast API and saves CSVs to `/data/raw`.
3.  **Spark (`spark`)**: Ephemeral Job. PySpark script that cleans, transforms, and computes metrics, saving Parquet to `/data/processed`.
4.  **Dashboard (`dashboard`)**: Streamlit app that visualizes the processed data.

All data is stored in the `./data` directory (bind-mounted to `/data` in containers), allowing you to inspect files locally.

### Tech Stack
-   **Orchestration**: Apache Airflow 2.7+ (DockerOperator)
-   **Ingestion**: Python, Requests, Pandas
-   **Processing**: PySpark 3.x (Local mode)
-   **Visualization**: Streamlit
-   **Infrastructure**: Docker, Docker Compose

## 🚀 How to Run

### Prerequisites
-   Docker and Docker Compose installed.
-   **Important**: Ensure your file sharing settings in Docker Desktop allow mounting the project directory, as Airflow needs to mount the host path to spawn sibling containers.

### Steps
1.  **Build and Start Services**:
    ```bash
    docker-compose up --build
    ```
    *Note: `ingestion` and `spark` services are defined with `profiles: ["jobs"]`, so they will NOT start automatically. Airflow will launch them.*

2.  **Access Airflow**:
    -   Go to [http://localhost:8080](http://localhost:8080)
    -   Username: `admin`
    -   Password: `admin`

3.  **Trigger the Pipeline**:
    -   Enable the `f1_pipeline_dag`.
    -   Click the "Trigger DAG" (Play) button.
    -   Wait for `ingest_f1_data` and `process_f1_data` tasks to complete (Green).

4.  **View Analytics**:
    -   Go to [http://localhost:8501](http://localhost:8501)
    -   The dashboard will show Standings, Consistency metrics, and Reliability scores.

5.  **Inspect Data**:
    -   Check `./data/raw` for CSV files (drivers, constructors, races, results).
    -   Check `./data/processed` for Parquet files.

## 📊 Analytics & Metrics
The pipeline calculates the following metrics:
-   **Driver Standings**: Total points by season.
-   **Constructor Standings**: Total points by constructor.
-   **Wins**: Top drivers by total race wins.
-   **Points Progression**: Cumulative points over the race season (Line chart).
-   **Driver Consistency Index**: Based on the variance of finishing positions. (Lower variance = Higher consistency).
-   **Constructor Reliability**: Percentage of races where the car finished (non-DNF).
-   **Points Efficiency**: Average points scored per race entered.

## ⚠️ Honesty Note
“This platform is refresh-based (near–real-time). Due to API limitations and infrastructure costs, the pipeline is triggered manually via Airflow rather than continuously streaming live race data.”

*Note for Reviewers*: The system uses `DockerOperator` to run tasks, requiring a Docker socket mount and a host path mapping.
