import asyncio
import time

import requests

from mcp_pinot.config import load_pinot_config
from mcp_pinot.pinot_client import PinotClient


def wait_for_pinot_ready():
    max_retries = 30
    retry_interval = 2

    for i in range(max_retries):
        try:
            response = requests.get("http://localhost:9000/health", timeout=10)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(retry_interval)
    return False


async def main():
    if not wait_for_pinot_ready():
        print("Pinot is not ready after maximum retries")
        return

    try:
        # Initialize Pinot client
        config = load_pinot_config()
        pinot = PinotClient(config)

        # Test connection
        connection_result = pinot.test_connection()
        print("Connection result:", connection_result)

        # Execute a query
        result = pinot.execute_query("SELECT * FROM airlineStats LIMIT 50")
        print("Query result:", result)

        # Verify the result contains our sample data
        if not result or "Carrier" not in str(result):
            raise Exception("Expected sample column not found in query results")

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
