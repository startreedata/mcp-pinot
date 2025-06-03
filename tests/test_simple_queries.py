#!/usr/bin/env python3

import os
import sys
import time
from dotenv import load_dotenv

# Add the project directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_pinot.utils.pinot_client import Pinot

def main():
    load_dotenv()
    
    print("Simple Query Test")
    print("=" * 40)
    
    pinot = Pinot()
    
    # Get a table name first
    print("Getting tables...")
    tables_result = pinot.handle_tool("list-tables", {})
    tables = eval(tables_result[0].text)
    first_table = tables[0]
    print(f"Testing with table: {first_table}")
    
    # Test the problematic query
    query = f"SELECT COUNT(*) FROM {first_table}"
    print(f"\nTesting query: {query}")
    
    start_time = time.time()
    try:
        result = pinot.handle_tool("read-query", {"query": query})
        end_time = time.time()
        duration = end_time - start_time
        
        result_data = eval(result[0].text)
        print(f"✅ SUCCESS: {len(result_data)} rows in {duration:.2f}s")
        print(f"Result: {result_data}")
        
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"❌ FAILED after {duration:.2f}s")
        print(f"Error: {e}")

if __name__ == "__main__":
    main() 
