from django.test.client import RequestFactory
from django.test.utils import CaptureQueriesContext
from django.db import connection
import pytest

from .fake_project import TestItem, schema


@pytest.fixture
def sample_data():
    TestItem.objects.all().delete()  # Clear existing data
    items = [
        TestItem(name="Apple", value=10),
        TestItem(name="Banana", value=5),
        TestItem(name="Cherry", value=15),
        TestItem(name="Date", value=8),
        TestItem(name="Elderberry", value=12),
        TestItem(name="Fig", value=3),
        TestItem(name="Grape", value=20),
        TestItem(name="Honeydew", value=7),
    ]
    TestItem.objects.bulk_create(items)
    return items


@pytest.fixture
def client():
    """GraphQL client with Django request context"""
    from graphene.test import Client

    factory = RequestFactory()
    request = factory.get("/")

    _client = Client(schema)

    original_execute = _client.execute

    def execute_with_context(query, variables=None):
        return original_execute(query, variables=variables, context=request)

    _client.execute = execute_with_context
    return _client


@pytest.mark.django_db
class TestPaginationE2E:
    """End-to-end tests for pagination library"""

    def test_basic_query_all_results(self, client, sample_data):
        """Test query without pagination returns all items"""
        query = """
        query {
            items {
                results {
                    id
                    name
                    value
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 8
        assert data["totalCount"] == 8
        assert data["pageInfo"]["hasNextPage"] == False
        assert data["pageInfo"]["hasPreviousPage"] == False

    def test_pagination_first_page(self, client, sample_data):
        """Test first page with limit"""
        query = """
        query {
            items(limit: 3) {
                results {
                    id
                    name
                    value
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 3
        assert data["totalCount"] == 8
        assert data["pageInfo"]["hasNextPage"] == True
        assert data["pageInfo"]["hasPreviousPage"] == False

    def test_pagination_middle_page(self, client, sample_data):
        """Test middle page with limit and offset"""
        query = """
        query {
            items(limit: 3, offset: 3) {
                results {
                    id
                    name
                    value
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 3
        assert data["totalCount"] == 8
        assert data["pageInfo"]["hasNextPage"] == True
        assert data["pageInfo"]["hasPreviousPage"] == True

    def test_pagination_last_page(self, client, sample_data):
        """Test last page"""
        query = """
        query {
            items(limit: 3, offset: 6) {
                results {
                    id
                    name
                    value
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 2  # Only 2 items left
        assert data["totalCount"] == 8
        assert data["pageInfo"]["hasNextPage"] == False
        assert data["pageInfo"]["hasPreviousPage"] == True

    def test_ordering_ascending(self, client, sample_data):
        """Test ordering by name ascending"""
        query = """
        query {
            items(limit: 4, ordering: "name, asc") {
                results {
                    id
                    name
                    value
                }
                totalCount
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 4
        assert data["totalCount"] == 8

        # Check alphabetical order
        names = [item["name"] for item in data["results"]]
        assert names == sorted(names)

    def test_ordering_descending(self, client, sample_data):
        """Test ordering by value descending"""
        query = """
        query {
            items(limit: 4, ordering: "value, desc") {
                results {
                    id
                    name
                    value
                }
                totalCount
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 4
        assert data["totalCount"] == 8

        # Check descending order by value
        values = [item["value"] for item in data["results"]]
        assert values == sorted(values, reverse=True)

    def test_variables(self, client, sample_data):
        """Test with GraphQL variables"""
        query = """
        query GetItems($limit: Int, $offset: Int, $ordering: String) {
            items(limit: $limit, offset: $offset, ordering: $ordering) {
                results {
                    id
                    name
                    value
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        variables = {"limit": 2, "offset": 2, "ordering": "value, asc"}

        result = client.execute(query, variables=variables)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 2
        assert data["totalCount"] == 8
        assert data["pageInfo"]["hasNextPage"] == True
        assert data["pageInfo"]["hasPreviousPage"] == True

    def test_empty_results(self, client):
        """Test with empty data"""
        TestItem.objects.all().delete()

        query = """
        query {
            items(limit: 5) {
                results {
                    id
                    name
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 0
        assert data["totalCount"] == 0
        assert data["pageInfo"]["hasNextPage"] == False
        assert data["pageInfo"]["hasPreviousPage"] == False

    def test_large_offset(self, client, sample_data):
        """Test offset beyond available data"""
        query = """
        query {
            items(limit: 5, offset: 20) {
                results {
                    id
                    name
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        data = result["data"]["items"]
        assert len(data["results"]) == 0
        assert data["totalCount"] == 8
        assert data["pageInfo"]["hasNextPage"] == False
        assert data["pageInfo"]["hasPreviousPage"] == True


@pytest.mark.django_db
class TestErrorHandling:
    """Test error scenarios"""

    def test_negative_limit(self, client, sample_data):
        """Test negative limit raises error"""
        query = """
        query {
            items(limit: -1) {
                results {
                    id
                }
            }
        }
        """

        result = client.execute(query)
        assert result.get("errors")
        assert any("positive integer" in str(error) for error in result["errors"])

    def test_zero_limit(self, client, sample_data):
        """Test zero limit raises error"""
        query = """
        query {
            items(limit: 0) {
                results {
                    id
                }
            }
        }
        """

        result = client.execute(query)
        assert result.get("errors")
        assert any("positive integer" in str(error) for error in result["errors"])


@pytest.mark.django_db
class TestPageInfo:
    """Test PageInfo functionality"""

    def test_page_info_structure(self, client, sample_data):
        """Test PageInfo fields are properly exposed"""
        query = """
        query {
            items(limit: 3, offset: 2) {
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
            }
        }
        """

        result = client.execute(query)
        assert not result.get("errors"), f"Errors: {result.get('errors')}"

        page_info = result["data"]["items"]["pageInfo"]
        assert isinstance(page_info["hasNextPage"], bool)
        assert isinstance(page_info["hasPreviousPage"], bool)
        assert page_info["hasNextPage"] == True
        assert page_info["hasPreviousPage"] == True


@pytest.mark.django_db
class TestTotalCount:
    """Test total count functionality"""

    def test_total_count_consistent(self, client, sample_data):
        """Test total count is consistent across pages"""
        queries = [
            "query { items(limit: 3, offset: 0) { totalCount } }",
            "query { items(limit: 3, offset: 3) { totalCount } }",
            "query { items(limit: 3, offset: 6) { totalCount } }",
        ]

        for query in queries:
            result = client.execute(query)
            assert not result.get("errors"), f"Errors: {result.get('errors')}"
            assert result["data"]["items"]["totalCount"] == 8


@pytest.mark.django_db
class TestCountOptimization:
    """Test count query optimization when items < limit"""

    def test_last_page_skips_count_query(self, client, sample_data):
        """Test that COUNT query is skipped when on last page with items < limit"""
        # Query the last page where we have 2 items but limit is 3
        query = """
        query {
            items(limit: 3, offset: 6) {
                results {
                    id
                    name
                    value
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        with CaptureQueriesContext(connection) as context:
            result = client.execute(query)
            assert not result.get("errors"), f"Errors: {result.get('errors')}"

            # Verify the results are correct
            data = result["data"]["items"]
            assert len(data["results"]) == 2
            assert data["totalCount"] == 8
            assert data["pageInfo"]["hasNextPage"] == False
            assert data["pageInfo"]["hasPreviousPage"] == True

            # Check that no COUNT query was executed
            # We should only have one SELECT query for fetching the items
            queries = [q['sql'] for q in context.captured_queries]
            count_queries = [q for q in queries if 'COUNT' in q.upper()]

            assert len(count_queries) == 0, f"Expected no COUNT queries, but found: {count_queries}"

    def test_first_page_partial_skips_count_query(self, client):
        """Test that COUNT query is skipped on first page when total items < limit"""
        # Create only 3 items but request limit of 5
        TestItem.objects.all().delete()
        items = [
            TestItem(name="Apple", value=10),
            TestItem(name="Banana", value=5),
            TestItem(name="Cherry", value=15),
        ]
        TestItem.objects.bulk_create(items)

        query = """
        query {
            items(limit: 5, offset: 0) {
                results {
                    id
                    name
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        with CaptureQueriesContext(connection) as context:
            result = client.execute(query)
            assert not result.get("errors"), f"Errors: {result.get('errors')}"

            # Verify the results are correct
            data = result["data"]["items"]
            assert len(data["results"]) == 3
            assert data["totalCount"] == 3
            assert data["pageInfo"]["hasNextPage"] == False
            assert data["pageInfo"]["hasPreviousPage"] == False

            # Check that no COUNT query was executed
            queries = [q['sql'] for q in context.captured_queries]
            count_queries = [q for q in queries if 'COUNT' in q.upper()]

            assert len(count_queries) == 0, f"Expected no COUNT queries, but found: {count_queries}"

    def test_middle_page_with_full_limit_does_count_query(self, client, sample_data):
        """Test that COUNT query IS executed when we get exactly 'limit' items (not on last page)"""
        # Query a middle page where we'll get exactly 'limit' items
        # Since items == limit, we need COUNT to check if there are more pages
        query = """
        query {
            items(limit: 3, offset: 3) {
                results {
                    id
                    name
                }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                }
                totalCount
            }
        }
        """

        with CaptureQueriesContext(connection) as context:
            result = client.execute(query)
            assert not result.get("errors"), f"Errors: {result.get('errors')}"

            # Verify the results are correct
            data = result["data"]["items"]
            assert len(data["results"]) == 3
            assert data["totalCount"] == 8
            assert data["pageInfo"]["hasNextPage"] == True
            assert data["pageInfo"]["hasPreviousPage"] == True

            # Check that a COUNT query WAS executed (since we got exactly 'limit' items)
            queries = [q['sql'] for q in context.captured_queries]
            count_queries = [q for q in queries if 'COUNT' in q.upper()]

            assert len(count_queries) >= 1, f"Expected at least one COUNT query for middle page, but found: {count_queries}"
