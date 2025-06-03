#!/bin/sh
echo "Starting MCP Pinot Server"

# Load environment variables from .env file if it exists
if [ -f /app/config/.env ]; then
    echo "Loading environment variables from /app/config/.env"
    export $(cat /app/config/.env | xargs)
fi

# Check if we're in development mode (if .env exists in current directory)
if [ -f .env ]; then
    echo "Loading environment variables from .env"
    export $(cat .env | xargs)
fi

# Run the CLI with unbuffered output
PYTHONUNBUFFERED=1 python -m mcp_pinot.cli 
