#!/usr/bin/env python3

import os
import sys
import time
from dotenv import load_dotenv

# Add the project directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_pinot.utils.pinot_client import Pinot

def test_simple_queries():
    """Test various simple queries to identify timeout patterns"""
    load_dotenv()
    
    print("Testing Read Query Functionality...")
    print("=" * 60)
    
    pinot = Pinot()
    
    # Get list of tables first
    print("📋 Getting list of tables...")
    try:
        tables_result = pinot.handle_tool("list-tables", {})
        tables_text = tables_result[0].text
        tables = eval(tables_text)  # Convert string representation back to list
        print(f"✅ Found {len(tables)} tables")
        print(f"First 5 tables: {tables[:5]}")
    except Exception as e:
        print(f"❌ Failed to get tables: {e}")
        return
    
    # Test queries with different complexity levels
    test_queries = [
        ("Simple SELECT 1", "SELECT 1"),
        ("Simple COUNT on first table", f"SELECT COUNT(*) FROM {tables[0]}"),
        ("LIMIT 5 from first table", f"SELECT * FROM {tables[0]} LIMIT 5"),
        ("LIMIT 1 from first table", f"SELECT * FROM {tables[0]} LIMIT 1"),
    ]
    
    for query_name, query in test_queries:
        print(f"\n🔍 Testing: {query_name}")
        print(f"Query: {query}")
        print("-" * 40)
        
        start_time = time.time()
        try:
            result = pinot.handle_tool("read-query", {"query": query})
            end_time = time.time()
            duration = end_time - start_time
            
            # Parse the result
            result_text = result[0].text
            result_data = eval(result_text)  # Convert string representation back to data
            
            print(f"✅ Query executed successfully in {duration:.2f} seconds")
            print(f"📊 Result: {len(result_data)} rows")
            if result_data:
                print(f"🔍 Sample data: {result_data[0] if len(result_data) > 0 else 'No data'}")
                
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            print(f"❌ Query failed after {duration:.2f} seconds")
            print(f"🚨 Error: {e}")
            
        print(f"⏱️  Duration: {duration:.2f} seconds")

def test_table_details():
    """Test table details functionality"""
    load_dotenv()
    
    print("\n" + "=" * 60)
    print("Testing Table Details Functionality...")
    print("=" * 60)
    
    pinot = Pinot()
    
    # Get first table name
    try:
        tables_result = pinot.handle_tool("list-tables", {})
        tables = eval(tables_result[0].text)
        first_table = tables[0]
        
        print(f"🔍 Getting details for table: {first_table}")
        
        start_time = time.time()
        result = pinot.handle_tool("table-details", {"tableName": first_table})
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ Table details retrieved in {duration:.2f} seconds")
        result_data = eval(result[0].text)
        print(f"📊 Table details keys: {list(result_data.keys())}")
        
    except Exception as e:
        end_time = time.time()
        duration = end_time - start_time
        print(f"❌ Table details failed after {duration:.2f} seconds")
        print(f"🚨 Error: {e}")

def test_connection_reuse():
    """Test if connection reuse is working properly"""
    print("\n" + "=" * 60)
    print("Testing Connection Reuse...")
    print("=" * 60)
    
    pinot = Pinot()
    
    # Run multiple simple queries to test connection reuse
    for i in range(3):
        print(f"\n🔄 Query {i+1}/3")
        start_time = time.time()
        try:
            result = pinot.handle_tool("read-query", {"query": "SELECT 1"})
            end_time = time.time()
            duration = end_time - start_time
            print(f"✅ Query {i+1} completed in {duration:.2f} seconds")
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            print(f"❌ Query {i+1} failed after {duration:.2f} seconds: {e}")

def main():
    """Run all tests"""
    test_simple_queries()
    test_table_details()
    test_connection_reuse()
    
    print("\n" + "=" * 60)
    print("🏁 Test completed!")
    print("=" * 60)

if __name__ == "__main__":
    main() 
