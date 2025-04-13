# Airflow Pipelines

### Key Parts in the Docker Compose YAML:

1. **Airflow Common Configuration (`x-airflow-common`)**:
   - The configuration is split into a reusable section (`&airflow-common`) which can be reused across multiple services like `airflow-webserver`, `airflow-scheduler`, `airflow-worker`, etc.
   - It configures the base image (`apache/airflow:2.10.5`), sets environment variables related to Airflow’s configuration (e.g., **executor**, **database connection**, **Celery broker URL**, etc.).
   - It defines volumes for DAGs, logs, configuration files, and plugins.

2. **PostgreSQL (Airflow Metadata Database)**:
   - PostgreSQL is used as the backend for Airflow's metadata database.
   - It is configured to use the `airflow` user and password and the `airflow` database.
   - A health check is defined to ensure PostgreSQL is available.

3. **Redis (Celery Broker)**:
   - Redis is used as the Celery broker for managing task queues.
   - A health check is set up to ensure Redis is available.

4. **Airflow Webserver**:
   - The `airflow-webserver` service runs the Airflow web interface.
   - It exposes port 8080 to allow access to the UI.
   - A health check is added to ensure the webserver is running properly.

5. **Airflow Scheduler**:
   - The `airflow-scheduler` service schedules and triggers tasks based on the DAGs.
   - It has its own health check to monitor its status.

6. **Airflow Worker (Celery Worker)**:
   - The `airflow-worker` service is responsible for executing the tasks in the CeleryExecutor.
   - Health checks are added to ensure that the worker can communicate with the Celery broker and that it's able to process tasks.

7. **Airflow Triggerer**:
   - The `airflow-triggerer` service is used for triggering certain types of jobs.
   - It has its own health check.

8. **Airflow Initialization** (`airflow-init`):
   - This service initializes the Airflow database and sets up the initial admin user (if specified).
   - It performs some pre-checks to ensure system resources are adequate for running Airflow.

9. **Flower (Optional Monitoring)**:
   - Flower is a real-time web-based monitoring tool for Celery.
   - It's configured to run on port 5555.
   - You can enable Flower by running `docker-compose --profile flower up` or by explicitly starting it.
