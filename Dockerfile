FROM python:3.11-slim

# 1. Spark requires Java. Install OpenJDK.
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

# 2. Copy the high-speed uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 3. Use an isolated directory so Kestra's auto-mount can't shadow it
WORKDIR /opt/pipeline

# 4. Copy project configuration and install dependencies
COPY pyproject.toml uv.lock ./
RUN uv pip install --system -r pyproject.toml

# 5. Copy dataset, output folder, and script into the container
COPY region_lookup.csv /opt/pipeline/
COPY spark_output/ /opt/pipeline/spark_output/
COPY spark_jobs/03_minio_pipeline.py /opt/pipeline/spark_jobs/

# 6. Run the script explicitly using its absolute path
CMD ["python", "/opt/pipeline/spark_jobs/03_minio_pipeline.py"]