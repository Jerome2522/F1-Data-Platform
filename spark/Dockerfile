FROM python:3.9-slim

# Install OpenJDK 17
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

RUN pip install --no-cache-dir pyspark pandas pyarrow

COPY process_f1_data.py .

CMD ["python", "process_f1_data.py"]
