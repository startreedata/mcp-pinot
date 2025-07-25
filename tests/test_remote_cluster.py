#!/usr/bin/env python3
"""
Test script for MCP Pinot server functionality against remote StarTree Cloud cluster.
"""

import asyncio
import json

import pytest

from mcp_pinot.config import load_pinot_config
from mcp_pinot.pinot_client import PinotClient


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
async def test_connection_test(pinot):
    """Test Pinot connection."""
    print("\n🔧 Testing connection...")
    try:
        result = pinot.test_connection()
        print(f"✅ Connection test successful: {result}")
        return True
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False


@pytest.mark.skip(reason="Integration test requiring live Pinot cluster")
async def test_list_tables(pinot):
    """Test listing tables."""
    print("\n📋 Testing table listing...")
    try:
        result = pinot.get_tables()
        if result:
            tables = str(result).split("\n")
            print("✅ Found tables:")
            for table in tables[:5]:  # Show first 5 tables
                if table.strip():
                    print(f"   - {table.strip()}")
            if len(tables) > 5:
                print(f"   ... and {len(tables) - 5} more tables")
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
        result = pinot.get_table_detail(table_name)
        if result:
            print(f"✅ Got table details for {table_name}")
            details = str(result)
            if len(details) > 200:
                print(f"   Details: {details[:200]}...")
            else:
                print(f"   Details: {details}")
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
        result = pinot.execute_query(query)

        if result:
            print("✅ Query executed successfully")
            response = str(result)
            if len(response) > 300:
                print(f"   Result: {response[:300]}...")
            else:
                print(f"   Result: {response}")
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
        result = pinot.execute_query(query)

        if result:
            print("✅ Sample data query executed successfully")
            response = str(result)
            try:
                # Try to parse as JSON to see structure
                data = json.loads(response)
                if isinstance(data, list) and len(data) > 0:
                    print(f"   Retrieved {len(data)} records")
                    keys = list(data[0].keys()) if data[0] else "No keys"
                    print(f"   Sample record keys: {keys}")
                else:
                    print(f"   Data: {response[:200]}...")
            except Exception:
                print(f"   Raw response: {response[:200]}...")
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
        result = pinot.test_connection()
        if result:
            print("✅ Connection health check passed")
            print(f"   Status: {result}")
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
        test_connection_test(pinot_client),
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
        print(
            "\n🎉 All tests passed! MCP Pinot server is working "
            "correctly with remote Pinot cluster."
        )
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check the errors above.")


if __name__ == "__main__":
    asyncio.run(main())
