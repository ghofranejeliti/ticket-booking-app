FROM python:3.11-slim
WORKDIR /app
COPY transaction.py .
RUN pip install --no-cache-dir flask kafka-python requests psycopg2-binary
CMD ["python", "transaction.py"]