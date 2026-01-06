FROM apache/airflow:2.9.1

# Switch to the airflow user (standard practice)
USER airflow

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt