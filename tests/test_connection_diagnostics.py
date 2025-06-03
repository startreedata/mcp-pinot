#!/usr/bin/env python3

import os
import sys
from dotenv import load_dotenv

# Add the project directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_pinot.utils.pinot_client import Pinot

def main():
    # Load environment variables
    load_dotenv()
    
    print("Testing Pinot Connection...")
    print("=" * 50)
    
    # Create Pinot client
    pinot = Pinot()
    
    # Test connection
    result = pinot.test_connection()
    
    print("Connection Test Results:")
    print("-" * 30)
    for key, value in result.items():
        if key == "config":
            print(f"{key}:")
            for config_key, config_value in value.items():
                print(f"  {config_key}: {config_value}")
        else:
            print(f"{key}: {value}")
    
    print("\n" + "=" * 50)
    
    if result["connection_test"]:
        print("✅ Connection test PASSED")
    else:
        print("❌ Connection test FAILED")
        if result["error"]:
            print(f"Error: {result['error']}")
    
    if result["query_test"]:
        print("✅ Query test PASSED")
    else:
        print("❌ Query test FAILED")
    
    if result["tables_test"]:
        print("✅ Tables listing PASSED")
        print(f"Found {result.get('tables_count', 0)} tables")
    else:
        print("❌ Tables listing FAILED")

if __name__ == "__main__":
    main() 
