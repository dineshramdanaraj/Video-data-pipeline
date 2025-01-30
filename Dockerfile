FROM apache/airflow:2.10.4

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Switch back to airflow user
USER airflow

RUN airflow db init

RUN pip install poetry
# Copy poetry files first
COPY pyproject.toml poetry.lock /opt/airflow/

# Set working directory
WORKDIR /opt/airflow
COPY datalake /opt/airflow/datalake/
COPY config/airflow.cfg /opt/airflow/airflow.cfg
COPY client.properties /opt/airflow/

# Create necessary directories
RUN mkdir -p /opt/airflow/video_directory/local_staging \
    && mkdir -p /opt/airflow/video_directory/local_sink

# Set environment variables
ENV PYTHONPATH=/opt/airflow


# Configure Poetry and install dependencies without creating virtualenv
RUN poetry config virtualenvs.create false && poetry install --only main

# Copy the datalake package


# Install the root package


