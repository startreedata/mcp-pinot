#!/bin/sh
echo "Starting server"

# Load environment variables from .env file if it exists
if [ -f /app/config/.env ]; then
    echo "Loading environment variables from /app/config/.env"
    export $(cat /app/config/.env | xargs)
fi

# Run Python with unbuffered output
PYTHONUNBUFFERED=1 python -c "from mcp_pinot.server import main; main()" 
