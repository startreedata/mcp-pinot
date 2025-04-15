import asyncio
from mcp_pinot.utils.pinot_client import Pinot
import json
import requests
import time

def wait_for_pinot_ready():
    max_retries = 30
    retry_interval = 2
    
    for i in range(max_retries):
        try:
            response = requests.get("http://localhost:9000/health")
            if response.status_code == 200:
                return True
        except:
            pass
        time.sleep(retry_interval)
    return False

async def main():
    if not wait_for_pinot_ready():
        print("Pinot is not ready after maximum retries")
        return
    
    try:
        # Initialize Pinot client
        pinot = Pinot()
        
        # List available tools
        tools = pinot.list_tools()
        print("Available tools:", tools)
        
        # Execute a query
        result = pinot.handle_tool("read-query", {
            "query": "SELECT * FROM airlineStats LIMIT 50"
        })
        print("Query result:", result)
        
        # Verify the result contains our sample data
        if not result or not any("Carrier" in str(r) for r in result):
            raise Exception("Expected sample column not found in query results")
            
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
