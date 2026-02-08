# Use Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY producer/ .

# Copy shared schemas
COPY ../kafka-consumer/schemas ./schemas

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose port (if needed for health checks)
EXPOSE 8080

# Run the worker
CMD ["python", "producer.py"]
