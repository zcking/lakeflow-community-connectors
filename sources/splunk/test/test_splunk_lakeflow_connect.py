import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch

# Import test suite and connector
import tests.test_suite as test_suite
from tests.test_suite import LakeflowConnectTester
from tests.test_utils import load_config
from sources.splunk.splunk import LakeflowConnect
from pyspark.sql.types import StructType, IntegerType


# ============================================================================
# Integration Tests (using the standard test suite)
# ============================================================================

def test_splunk_connector():
    """Test the Splunk connector using the test suite"""
    # Load configuration
    parent_dir = Path(__file__).parent.parent
    config_path = parent_dir / "configs" / "dev_config.json"

    config = load_config(config_path)

    # Skip test if no credentials are provided
    if not config.get("api_token") or not config.get("base_url"):
        pytest.skip("Skipping integration test: No credentials in dev_config.json")

    # Inject the LakeflowConnect class into test_suite module's namespace
    # This is required because test_suite.py expects LakeflowConnect to be available
    test_suite.LakeflowConnect = LakeflowConnect

    # Create tester with the config
    tester = LakeflowConnectTester(config)

    # Run all tests
    report = tester.run_all_tests()

    # Print the report
    tester.print_report(report, show_details=True)

    # Assert that all tests passed
    assert report.passed_tests == report.total_tests, (
        f"Test suite had failures: {report.failed_tests} failed, {report.error_tests} errors"
    )


# ============================================================================
# Unit Tests
# ============================================================================

class TestSplunkConnectorInitialization:
    """Tests for connector initialization"""

    def test_init_with_valid_credentials(self):
        """Test initialization with valid api_token"""
        options = {
            "api_token": "test_token_123",
            "base_url": "https://api.us1.signalfx.com"
        }
        connector = LakeflowConnect(options)

        assert connector.api_token == "test_token_123"
        assert connector.base_url == "https://api.us1.signalfx.com"
        assert connector.headers["X-SF-TOKEN"] == "test_token_123"
        assert connector.page_size == 100

    def test_init_with_default_base_url(self):
        """Test initialization uses default base_url if not provided"""
        options = {"api_token": "test_token_123"}
        connector = LakeflowConnect(options)

        assert connector.base_url == "https://api.us1.signalfx.com"

    def test_init_without_api_token_raises_error(self):
        """Test initialization fails without api_token"""
        options = {}

        with pytest.raises(ValueError, match="Missing required option: 'api_token'"):
            LakeflowConnect(options)

    def test_init_with_none_api_token_raises_error(self):
        """Test initialization fails with None api_token"""
        options = {"api_token": None}

        with pytest.raises(ValueError, match="Missing required option: 'api_token'"):
            LakeflowConnect(options)

    def test_init_sets_correct_headers(self):
        """Test initialization sets correct authentication headers"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        assert "X-SF-TOKEN" in connector.headers
        assert connector.headers["X-SF-TOKEN"] == "test_token"
        assert connector.headers["Content-Type"] == "application/json"


class TestListTables:
    """Tests for list_tables method"""

    def test_list_tables_returns_all_tables(self):
        """Test list_tables returns the correct table list"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        tables = connector.list_tables()

        assert isinstance(tables, list)
        assert len(tables) == 4
        assert "members" in tables
        assert "teams" in tables
        assert "dashboards" in tables
        assert "metrics" in tables

    def test_list_tables_returns_strings(self):
        """Test list_tables returns list of strings"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        tables = connector.list_tables()

        for table in tables:
            assert isinstance(table, str)


class TestGetTableSchema:
    """Tests for get_table_schema method"""

    def test_get_table_schema_members(self):
        """Test schema for members table"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        schema = connector.get_table_schema("members")

        assert isinstance(schema, StructType)
        assert "id" in schema.fieldNames()
        assert "email" in schema.fieldNames()
        assert "fullName" in schema.fieldNames()
        assert "organizationId" in schema.fieldNames()
        assert "admin" in schema.fieldNames()
        assert "readOnly" in schema.fieldNames()
        assert "title_description" in schema.fieldNames()
        assert "role_description" in schema.fieldNames()
        assert schema["id"].nullable is False  # Primary key should not be nullable

    def test_get_table_schema_teams(self):
        """Test schema for teams table"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        schema = connector.get_table_schema("teams")

        assert isinstance(schema, StructType)
        assert "id" in schema.fieldNames()
        assert "name" in schema.fieldNames()
        assert "members" in schema.fieldNames()
        assert schema["id"].nullable is False

    def test_get_table_schema_dashboards(self):
        """Test schema for dashboards table"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        schema = connector.get_table_schema("dashboards")

        assert isinstance(schema, StructType)
        assert "id" in schema.fieldNames()
        assert "name" in schema.fieldNames()
        assert "description" in schema.fieldNames()
        assert schema["id"].nullable is False

    def test_get_table_schema_metrics(self):
        """Test schema for metrics table"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        schema = connector.get_table_schema("metrics")

        assert isinstance(schema, StructType)
        assert "name" in schema.fieldNames()
        assert "type" in schema.fieldNames()
        assert "description" in schema.fieldNames()
        assert schema["name"].nullable is False

    def test_get_table_schema_invalid_table(self):
        """Test schema for invalid table raises error"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        with pytest.raises(ValueError, match="Table 'invalid_table' is not supported"):
            connector.get_table_schema("invalid_table")

    def test_get_table_schema_uses_long_type_not_integer(self):
        """Test that all numeric fields use LongType, not IntegerType"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        for table in connector.list_tables():
            schema = connector.get_table_schema(table)
            for field in schema.fields:
                # Ensure no IntegerType is used
                assert not isinstance(field.dataType, IntegerType), \
                    f"Field {field.name} in table {table} uses IntegerType instead of LongType"


class TestReadTableMetadata:
    """Tests for read_table_metadata method"""

    def test_read_table_metadata_members(self):
        """Test metadata for members table"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        metadata = connector.read_table_metadata("members")

        assert isinstance(metadata, dict)
        assert metadata["primary_keys"] == ["id"]
        assert metadata["ingestion_type"] == "snapshot"

    def test_read_table_metadata_teams(self):
        """Test metadata for teams table"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        metadata = connector.read_table_metadata("teams")

        assert metadata["primary_keys"] == ["id"]
        assert metadata["ingestion_type"] == "snapshot"

    def test_read_table_metadata_dashboards(self):
        """Test metadata for dashboards table"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        metadata = connector.read_table_metadata("dashboards")

        assert metadata["primary_keys"] == ["id"]
        assert metadata["ingestion_type"] == "snapshot"

    def test_read_table_metadata_metrics(self):
        """Test metadata for metrics table"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        metadata = connector.read_table_metadata("metrics")

        assert metadata["primary_keys"] == ["name"]
        assert metadata["ingestion_type"] == "snapshot"

    def test_read_table_metadata_invalid_table(self):
        """Test metadata for invalid table raises error"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        with pytest.raises(ValueError, match="Table 'invalid_table' is not supported"):
            connector.read_table_metadata("invalid_table")


class TestMakeRequest:
    """Tests for _make_request helper method"""

    @patch('sources.splunk.splunk.requests.get')
    def test_make_request_success(self, mock_get):
        """Test successful API request"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": [{"id": "1"}]}
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        result = connector._make_request("/v2/organization/member")

        assert result == {"results": [{"id": "1"}]}
        mock_get.assert_called_once()

    @patch('sources.splunk.splunk.requests.get')
    def test_make_request_with_params(self, mock_get):
        """Test API request with parameters"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        result = connector._make_request("/v2/team", params={"offset": 0, "limit": 100})

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["params"] == {"offset": 0, "limit": 100}

    @patch('sources.splunk.splunk.requests.get')
    def test_make_request_includes_headers(self, mock_get):
        """Test API request includes authentication headers"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        options = {"api_token": "test_token_abc"}
        connector = LakeflowConnect(options)

        connector._make_request("/v2/organization/member")

        call_kwargs = mock_get.call_args[1]
        assert "headers" in call_kwargs
        assert call_kwargs["headers"]["X-SF-TOKEN"] == "test_token_abc"

    @patch('sources.splunk.splunk.requests.get')
    def test_make_request_401_error(self, mock_get):
        """Test 401 authentication error"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        with pytest.raises(Exception, match="SignalFx API authentication failed"):
            connector._make_request("/v2/organization/member")

    @patch('sources.splunk.splunk.requests.get')
    def test_make_request_403_error(self, mock_get):
        """Test 403 forbidden error"""
        mock_response = Mock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        with pytest.raises(Exception, match="SignalFx API access forbidden"):
            connector._make_request("/v2/organization/member")

    @patch('sources.splunk.splunk.requests.get')
    def test_make_request_generic_error(self, mock_get):
        """Test generic API error"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        with pytest.raises(Exception, match="SignalFx API error: 500"):
            connector._make_request("/v2/organization/member")


class TestReadMembers:
    """Tests for _read_members method"""

    @patch('sources.splunk.splunk.requests.get')
    def test_read_members_single_page(self, mock_get):
        """Test reading members with single page of results"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "member1",
                "email": "user1@example.com",
                "fullName": "User One",
                "organizationId": "org123",
                "created": 1234567890,
                "lastUpdated": 1234567900,
                "admin": True,
                "readOnly": False,
                "roles": []
            },
            {
                "id": "member2",
                "email": "user2@example.com",
                "fullName": "User Two",
                "organizationId": "org123",
                "created": 1234567891,
                "lastUpdated": 1234567901,
                "admin": False,
                "readOnly": True,
                "roles": []
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 2
        assert records[0]["id"] == "member1"
        assert records[0]["email"] == "user1@example.com"
        assert records[1]["id"] == "member2"
        assert offset == {"completed": True}

    @patch('sources.splunk.splunk.requests.get')
    def test_read_members_with_roles(self, mock_get):
        """Test reading members with roles array parsing"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "member1",
                "email": "user1@example.com",
                "roles": [
                    {"title": "Admin", "description": "Administrator role"},
                    {"title": "Developer", "description": "Developer role"}
                ]
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 1
        assert records[0]["title_description"] == "Admin,Developer"
        assert records[0]["role_description"] == "Administrator role,Developer role"

    @patch('sources.splunk.splunk.requests.get')
    def test_read_members_with_partial_roles(self, mock_get):
        """Test reading members with roles missing title or description"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "member1",
                "email": "user1@example.com",
                "roles": [
                    {"title": "Admin"},  # No description
                    {"description": "Test role"}  # No title
                ]
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 1
        assert records[0]["title_description"] == "Admin"
        assert records[0]["role_description"] == "Test role"

    @patch('sources.splunk.splunk.requests.get')
    def test_read_members_with_empty_roles(self, mock_get):
        """Test reading members with empty roles array"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "member1",
                "email": "user1@example.com",
                "roles": []
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 1
        assert records[0]["title_description"] is None
        assert records[0]["role_description"] is None

    @patch('sources.splunk.splunk.requests.get')
    def test_read_members_with_offset(self, mock_get):
        """Test reading members with start offset"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({"offset": 50})
        list(iterator)  # Consume iterator

        # Verify the API was called with the correct offset
        call_kwargs = mock_get.call_args[1]
        assert call_kwargs["params"]["offset"] == 50

    @patch('sources.splunk.splunk.requests.get')
    def test_read_members_pagination(self, mock_get):
        """Test reading members with pagination"""
        # First page
        first_response = Mock()
        first_response.status_code = 200
        first_response.json.return_value = {
            "results": [{"id": f"member{i}", "email": f"user{i}@example.com", "roles": []} for i in range(100)],
            "count": 150
        }

        # Second page (partial)
        second_response = Mock()
        second_response.status_code = 200
        second_response.json.return_value = {
            "results": [{"id": f"member{i}", "email": f"user{i}@example.com", "roles": []} for i in range(100, 150)],
            "count": 150
        }

        mock_get.side_effect = [first_response, second_response]

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 150
        assert offset == {"completed": True}
        assert mock_get.call_count == 2

    @patch('sources.splunk.splunk.requests.get')
    def test_read_members_empty_results(self, mock_get):
        """Test reading members with no results"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 0
        assert offset == {"completed": True}


class TestReadTeams:
    """Tests for _read_teams method"""

    @patch('sources.splunk.splunk.requests.get')
    def test_read_teams_success(self, mock_get):
        """Test reading teams successfully"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "team1",
                "name": "Engineering",
                "members": ["member1", "member2"],
                "description": "Engineering team",
                "created": 1234567890,
                "lastUpdated": 1234567900
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_teams({})
        records = list(iterator)

        assert len(records) == 1
        assert records[0]["id"] == "team1"
        assert records[0]["name"] == "Engineering"
        assert offset == {"completed": True}

    @patch('sources.splunk.splunk.requests.get')
    def test_read_teams_pagination(self, mock_get):
        """Test reading teams with pagination"""
        # First page
        first_response = Mock()
        first_response.status_code = 200
        first_response.json.return_value = {
            "results": [{"id": f"team{i}", "name": f"Team {i}"} for i in range(100)],
            "count": 120
        }

        # Second page
        second_response = Mock()
        second_response.status_code = 200
        second_response.json.return_value = {
            "results": [{"id": f"team{i}", "name": f"Team {i}"} for i in range(100, 120)],
            "count": 120
        }

        mock_get.side_effect = [first_response, second_response]

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_teams({})
        records = list(iterator)

        assert len(records) == 120
        assert mock_get.call_count == 2


class TestReadDashboards:
    """Tests for _read_dashboards method"""

    @patch('sources.splunk.splunk.requests.get')
    def test_read_dashboards_success(self, mock_get):
        """Test reading dashboards successfully"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "dashboard1",
                "name": "System Dashboard",
                "description": "Main system dashboard",
                "created": 1234567890,
                "lastUpdated": 1234567900,
                "groupId": "group1"
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_dashboards({})
        records = list(iterator)

        assert len(records) == 1
        assert records[0]["id"] == "dashboard1"
        assert records[0]["name"] == "System Dashboard"
        assert offset == {"completed": True}


class TestReadMetrics:
    """Tests for _read_metrics method"""

    @patch('sources.splunk.splunk.requests.get')
    def test_read_metrics_success(self, mock_get):
        """Test reading metrics successfully"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "name": "cpu.utilization",
                "type": "gauge",
                "description": "CPU utilization percentage",
                "created": 1234567890,
                "lastUpdated": 1234567900
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_metrics({})
        records = list(iterator)

        assert len(records) == 1
        assert records[0]["name"] == "cpu.utilization"
        assert records[0]["type"] == "gauge"
        assert offset == {"completed": True}


class TestReadTable:
    """Tests for read_table method"""

    @patch('sources.splunk.splunk.requests.get')
    def test_read_table_members(self, mock_get):
        """Test read_table routes to _read_members"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "member1", "roles": []}]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector.read_table("members", {})
        records = list(iterator)

        assert len(records) == 1
        assert isinstance(offset, dict)

    @patch('sources.splunk.splunk.requests.get')
    def test_read_table_teams(self, mock_get):
        """Test read_table routes to _read_teams"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "team1"}]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector.read_table("teams", {})
        records = list(iterator)

        assert len(records) == 1

    @patch('sources.splunk.splunk.requests.get')
    def test_read_table_dashboards(self, mock_get):
        """Test read_table routes to _read_dashboards"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "dashboard1"}]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector.read_table("dashboards", {})
        records = list(iterator)

        assert len(records) == 1

    @patch('sources.splunk.splunk.requests.get')
    def test_read_table_metrics(self, mock_get):
        """Test read_table routes to _read_metrics"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"name": "metric1"}]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector.read_table("metrics", {})
        records = list(iterator)

        assert len(records) == 1

    def test_read_table_invalid_table(self):
        """Test read_table with invalid table name"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        with pytest.raises(ValueError, match="Table 'invalid_table' is not supported"):
            connector.read_table("invalid_table", {})

    @patch('sources.splunk.splunk.requests.get')
    def test_read_table_returns_iterator(self, mock_get):
        """Test read_table returns proper iterator"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "member1", "roles": []}, {"id": "member2", "roles": []}]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector.read_table("members", {})

        # Iterator should be consumable
        assert hasattr(iterator, '__iter__')
        records = list(iterator)
        assert len(records) == 2

    @patch('sources.splunk.splunk.requests.get')
    def test_read_table_with_table_options(self, mock_get):
        """Test read_table accepts table_options parameter"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        # Should accept table_options without error
        iterator, offset = connector.read_table("members", {}, table_options={"some_option": "value"})
        list(iterator)  # Consume iterator

        assert offset == {"completed": True}


class TestEdgeCases:
    """Tests for edge cases and error conditions"""

    @patch('sources.splunk.splunk.requests.get')
    def test_handle_dict_response_format(self, mock_get):
        """Test handling response as dict with 'results' key"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [{"id": "member1", "roles": []}],
            "count": 1
        }
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 1

    @patch('sources.splunk.splunk.requests.get')
    def test_handle_list_response_format(self, mock_get):
        """Test handling response as direct list"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "member1", "roles": []}]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 1

    @patch('sources.splunk.splunk.requests.get')
    def test_handle_missing_optional_fields(self, mock_get):
        """Test handling records with missing optional fields"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "member1",
                # Missing optional fields like email, fullName, etc.
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        assert len(records) == 1
        assert records[0]["id"] == "member1"
        # Optional fields should be None or have default values
        assert records[0].get("email") is None
        assert records[0]["admin"] is False  # Default value

    @patch('sources.splunk.splunk.requests.get')
    def test_handle_invalid_roles_format(self, mock_get):
        """Test handling roles field with non-list value"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "member1",
                "roles": "not a list"  # Invalid format
            }
        ]
        mock_get.return_value = mock_response

        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        iterator, offset = connector._read_members({})
        records = list(iterator)

        # Should handle gracefully without crashing
        assert len(records) == 1
        assert records[0]["title_description"] is None
        assert records[0]["role_description"] is None

    def test_schema_primary_keys_match_metadata(self):
        """Test that primary keys in metadata exist in schema"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        for table in connector.list_tables():
            schema = connector.get_table_schema(table)
            metadata = connector.read_table_metadata(table)

            for pk in metadata["primary_keys"]:
                assert pk in schema.fieldNames(), \
                    f"Primary key '{pk}' not found in schema for table '{table}'"

    def test_all_tables_have_snapshot_ingestion(self):
        """Test that all tables use snapshot ingestion type"""
        options = {"api_token": "test_token"}
        connector = LakeflowConnect(options)

        for table in connector.list_tables():
            metadata = connector.read_table_metadata(table)
            assert metadata["ingestion_type"] == "snapshot", \
                f"Table '{table}' should use snapshot ingestion"
