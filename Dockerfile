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

# Create a non-root user with UID 1000
RUN groupadd -r -g 1000 appuser && useradd -r -g appuser -u 1000 -m appuser

# Set the HOME environment variable for the appuser
ENV HOME=/home/appuser

# Make the run script executable
RUN chmod +x /app/run.sh

# Change ownership of the entire app directory to the appuser
RUN chown -R appuser:appuser /app

# Switch to the non-root user
USER appuser

# Set the entry point to use the wrapper script
ENTRYPOINT ["/app/run.sh"]
