FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir requests pandas

COPY ingest_f1_data.py .

CMD ["python", "ingest_f1_data.py"]
