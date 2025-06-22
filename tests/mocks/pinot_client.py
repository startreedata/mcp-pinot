"""
Mock implementation of the PinotClient class for testing.
"""

class PinotClient:
    """
    Mock implementation of the PinotClient class.
    """
    def __init__(self):
        pass

    def execute_query(self, query):
        """
        Mock implementation of the execute_query method.
        Returns a sample result.
        """
        if "SELECT" not in query.upper():
            raise ValueError("Only SELECT queries are allowed")

        # Return a sample result
        return [
            {"id": 1, "name": "Test 1"},
            {"id": 2, "name": "Test 2"}
        ]
