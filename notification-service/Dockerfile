FROM python:3.11-slim
WORKDIR /app
COPY notification.py .
RUN pip install --no-cache-dir flask flask-cors kafka-python requests psycopg2-binary
CMD ["python", "notification.py"]