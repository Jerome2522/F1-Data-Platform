FROM apache/spark:3.5.0-python3

USER root

WORKDIR /app

# Install pandas dependencies if missing in base (usually numpy is there, but pandas might not be)
# Apache spark python image has pyspark in path if used via spark-submit, 
# but for direct python execution we need to ensure it's on PYTHONPATH or pip installed.
# Installing via pip ensures it works with standard python execution.
RUN pip install --no-cache-dir pyspark==3.5.0 pandas pyarrow

COPY process_f1_data.py .

CMD ["python3", "process_f1_data.py"]
