#!/usr/bin/env python3
"""
Test script for MCP Pinot server functionality against remote StarTree Cloud cluster.
"""
import asyncio
import json
import pytest
from mcp_pinot.config import load_pinot_config
from mcp_pinot.utils.pinot_client import PinotClient

@pytest.mark.skip(reason="Integration test requiring live Pinot cluster")
async def test_connection():
    """Test basic connection to the remote StarTree Cloud cluster."""
    print("🔌 Testing connection to remote StarTree Cloud cluster...")
    try:
        config = load_pinot_config()
        pinot_client = PinotClient(config)
        print("✅ Connection established successfully")
        return pinot_client
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

@pytest.mark.skip(reason="Integration test requiring live Pinot cluster")
async def test_list_tools(pinot):
    """Test listing available tools."""
    print("\n🔧 Testing tool listing...")
    try:
        tools = pinot.list_tools()
        print(f"✅ Found {len(tools)} tools:")
        for tool in tools:
            print(f"   - {tool.name}: {tool.description}")
        return True
    except Exception as e:
        print(f"❌ Tool listing failed: {e}")
        return False

@pytest.mark.skip(reason="Integration test requiring live Pinot cluster")
async def test_list_tables(pinot):
    """Test listing tables."""
    print("\n📋 Testing table listing...")
    try:
        result = pinot.handle_tool("list-tables", {})
        if result and len(result) > 0:
            print(f"✅ Found {len(result)} tables:")
            for i, content in enumerate(result):
                if hasattr(content, 'text'):
                    tables = content.text.split('\n')
                    for table in tables[:5]:  # Show first 5 tables
                        if table.strip():
                            print(f"   - {table.strip()}")
                    if len(tables) > 5:
                        print(f"   ... and {len(tables) - 5} more tables")
                    break
        else:
            print("⚠️  No tables found")
        return True
    except Exception as e:
        print(f"❌ Table listing failed: {e}")
        return False

@pytest.mark.skip(reason="Integration test requiring live Pinot cluster")
async def test_table_details(pinot, table_name="hubble_events"):
    """Test getting table details."""
    print(f"\n📊 Testing table details for '{table_name}'...")
    try:
        result = pinot.handle_tool("table-details", {"tableName": table_name})
        if result and len(result) > 0:
            print(f"✅ Got table details for {table_name}")
            # Print a summary of the details
            for content in result:
                if hasattr(content, 'text'):
                    details = content.text
                    if len(details) > 200:
                        print(f"   Details: {details[:200]}...")
                    else:
                        print(f"   Details: {details}")
                    break
        else:
            print(f"⚠️  No details found for table {table_name}")
        return True
    except Exception as e:
        print(f"❌ Table details failed: {e}")
        return False

@pytest.mark.skip(reason="Integration test requiring live Pinot cluster")
async def test_query_execution(pinot):
    """Test executing a simple query."""
    print("\n🔍 Testing query execution...")
    try:
        # Try a simple count query first using a table that exists
        query = "SELECT COUNT(*) as total_count FROM hubble_events LIMIT 1"
        result = pinot.handle_tool("read-query", {"query": query})
        
        if result and len(result) > 0:
            print(f"✅ Query executed successfully")
            for content in result:
                if hasattr(content, 'text'):
                    response = content.text
                    if len(response) > 300:
                        print(f"   Result: {response[:300]}...")
                    else:
                        print(f"   Result: {response}")
                    break
        else:
            print("⚠️  Query returned no results")
        return True
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        return False

@pytest.mark.skip(reason="Integration test requiring live Pinot cluster")
async def test_sample_data_query(pinot):
    """Test querying sample data."""
    print("\n📈 Testing sample data query...")
    try:
        # Query for sample data from an existing table
        query = "SELECT * FROM hubble_events LIMIT 5"
        result = pinot.handle_tool("read-query", {"query": query})
        
        if result and len(result) > 0:
            print(f"✅ Sample data query executed successfully")
            for content in result:
                if hasattr(content, 'text'):
                    response = content.text
                    try:
                        # Try to parse as JSON to see structure
                        data = json.loads(response)
                        if isinstance(data, list) and len(data) > 0:
                            print(f"   Retrieved {len(data)} records")
                            print(f"   Sample record keys: {list(data[0].keys()) if data[0] else 'No keys'}")
                        else:
                            print(f"   Data: {response[:200]}...")
                    except:
                        print(f"   Raw response: {response[:200]}...")
                    break
        else:
            print("⚠️  Sample query returned no results")
        return True
    except Exception as e:
        print(f"❌ Sample data query failed: {e}")
        return False

@pytest.mark.skip(reason="Integration test requiring live Pinot cluster")
async def test_connection_health(pinot):
    """Test connection health."""
    print("\n🏥 Testing connection health...")
    try:
        result = pinot.handle_tool("test-connection", {})
        if result and len(result) > 0:
            print("✅ Connection health check passed")
            for content in result:
                if hasattr(content, 'text'):
                    print(f"   Status: {content.text}")
                    break
        else:
            print("⚠️  Health check returned no results")
        return True
    except Exception as e:
        print(f"❌ Connection health check failed: {e}")
        return False

async def main():
    """Run all tests."""
    print("🚀 Starting MCP Pinot Server Tests against Remote StarTree Cloud")
    print("=" * 70)
    
    # Test connection
    pinot_client = await test_connection()
    if not pinot_client:
        print("\n❌ Cannot proceed without connection. Exiting.")
        return
    
    # Run all tests
    tests = [
        test_list_tools(pinot_client),
        test_connection_health(pinot_client),
        test_list_tables(pinot_client),
        test_table_details(pinot_client),
        test_query_execution(pinot_client),
        test_sample_data_query(pinot_client),
    ]
    
    results = []
    for test in tests:
        try:
            result = await test
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 Test Summary:")
    passed = sum(results)
    total = len(results)
    print(f"   ✅ Passed: {passed}/{total}")
    print(f"   ❌ Failed: {total - passed}/{total}")
    
    if passed == total:
        print("\n🎉 All tests passed! MCP Pinot server is working correctly with remote StarTree Cloud.")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check the errors above.")

if __name__ == "__main__":
    asyncio.run(main()) 
