FROM python:3.11-slim
WORKDIR /app

# Copy all necessary files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir flask kafka-python requests psycopg2-binary

# Expose Flask port
EXPOSE 5000

# Run database initialization first, then start Flask
CMD ["sh", "-c", "python init_db.py && python app_backend.py"]
