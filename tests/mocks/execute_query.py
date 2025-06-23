"""
Mock implementation of the execute_query function for testing.
"""


def execute_query(query):
    """
    Mock implementation of the execute_query function.
    Returns a sample result.
    """
    if "SELECT" not in query.upper():
        raise ValueError("Only SELECT queries are allowed")

    # Return a sample result
    return [{"id": 1, "name": "Test 1"}, {"id": 2, "name": "Test 2"}]
