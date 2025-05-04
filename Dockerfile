# Use Python 3.10 as the base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app/

# Install Python dependencies
RUN pip install --no-cache-dir .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Make the run script executable
RUN chmod +x /app/run.sh

# Create a directory for environment files
RUN mkdir -p /app/config

# Set the entry point to use the wrapper script
ENTRYPOINT ["/app/run.sh"] 