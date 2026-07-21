#!/bin/sh
echo "Starting server" >&2

# The Python dotenv parser handles quoting safely. Never source or shell-expand
# this file: values may contain spaces and shell metacharacters.
if [ -f /app/config/.env ]; then
    echo "Loading environment variables from /app/config/.env" >&2
fi

# Run Python with unbuffered output
exec python -c 'from dotenv import load_dotenv; load_dotenv("/app/config/.env", override=False); from mcp_pinot.server import main; main()'
