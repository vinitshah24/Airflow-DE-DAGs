# Use the official Apache Airflow image
FROM apache/airflow:2.10.5

# Set the working directory inside the container
WORKDIR /opt/airflow

# Copy the requirements.txt into the container
COPY requirements.txt .

# Install the Python dependencies from the requirements file
RUN pip install --no-cache-dir -r requirements.txt