import requests
from typing import Iterator
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
)


class LakeflowConnect:
    """
    SignalFx connector for Lakeflow.
    Retrieves organization members from SignalFx API.
    """

    def __init__(self, options: dict) -> None:
        """
        Initialize the SignalFx connector with connection parameters.

        Args:
            options: Dictionary containing:
                - api_token: SignalFx API token for authentication
                - base_url: SignalFx API base URL (default: https://api.us1.signalfx.com)
        """
        self.api_token = options.get("api_token")
        self.base_url = options.get("base_url", "https://api.us1.signalfx.com")

        if not self.api_token:
            raise ValueError("Missing required option: 'api_token'")

        # Set up headers for authentication
        self.headers = {
            "X-SF-TOKEN": self.api_token,
            "Content-Type": "application/json"
        }

        # Default page size for pagination
        self.page_size = 100

    def list_tables(self) -> list[str]:
        """
        Returns a list of available tables from the SignalFx API.

        Returns:
            List of table names: members, teams
        """
        return ["members", "teams"]

    def get_table_schema(self, table_name: str, table_options: dict[str, str] = {}) -> StructType:
        """
        Fetch the schema of a table.

        Args:
            table_name: The name of the table to fetch the schema for.
            table_options: Additional options for the table.

        Returns:
            A StructType object representing the schema of the table.
        """
        schemas = {
            "members": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("organizationId", StringType(), True),
                    StructField("fullName", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("lastUpdated", LongType(), True),
                    StructField("admin", BooleanType(), True),
                    StructField("readOnly", BooleanType(), True),
                    StructField("creator", StringType(), True),
                    StructField("title", StringType(), True),
                    StructField("roles", StringType(), True),
                    StructField("title_description", StringType(), True),
                    StructField("role_description", StringType(), True),
                ]
            ),
            "teams": StructType(
                [
                    StructField("id", StringType(), False),
                    StructField("name", StringType(), True),
                    StructField("members", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("created", LongType(), True),
                    StructField("lastUpdated", LongType(), True),
                    StructField("creator", StringType(), True),
                ]
            ),
        }

        if table_name not in schemas:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return schemas[table_name]

    def read_table_metadata(self, table_name: str, table_options: dict[str, str] = {}) -> dict:
        """
        Fetch the metadata of a table.

        Args:
            table_name: The name of the table to fetch the metadata for.
            table_options: Additional options for the table.

        Returns:
            A dictionary containing primary_keys and ingestion_type.
            SignalFx members use snapshot ingestion.
        """
        metadata = {
            "members": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
            "teams": {
                "primary_keys": ["id"],
                "ingestion_type": "snapshot",
            },
        }

        if table_name not in metadata:
            raise ValueError(f"Table '{table_name}' is not supported.")

        return metadata[table_name]

    def read_table(self, table_name: str, start_offset: dict, table_options: dict[str, str] = {}) -> tuple[Iterator[dict], dict]:
        """
        Read the records of a table and return an iterator of records and an offset.

        Args:
            table_name: The name of the table to read.
            start_offset: The offset to start reading from.
            table_options: Additional options for the table.

        Returns:
            An iterator of records in JSON format and an offset dictionary.
        """
        if table_name == "members":
            return self._read_members(start_offset)
        elif table_name == "teams":
            return self._read_teams(start_offset)
        else:
            raise ValueError(f"Table '{table_name}' is not supported.")

    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """
        Make a GET request to the SignalFx API.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}{endpoint}"

        if params is None:
            params = {}

        response = requests.get(
            url,
            headers=self.headers,
            params=params,
        )

        if response.status_code == 401:
            raise Exception("SignalFx API authentication failed. Check API token.")
        elif response.status_code == 403:
            raise Exception(
                "SignalFx API access forbidden. Ensure API token has appropriate permissions."
            )
        elif response.status_code != 200:
            raise Exception(f"SignalFx API error: {response.status_code} {response.text}")

        return response.json()

    def _read_members(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """
        Read organization members from SignalFx API.

        Args:
            start_offset: Offset containing pagination info (not used for full refresh)

        Returns:
            Iterator of member records and next offset
        """
        all_records = []

        # Call the SignalFx members API endpoint
        data = self._make_request("/v2/organization/member")
        
        # The API returns a list of member objects
        members = data if isinstance(data, list) else data.get("results", [])

        for member in members:
            record = {
                "id": member.get("id"),
                "organizationId": member.get("organizationId"),
                "fullName": member.get("fullName"),
                "email": member.get("email"),
                "created": member.get("created"),
                "lastUpdated": member.get("lastUpdated"),
                "admin": member.get("admin", False),
                "readOnly": member.get("readOnly", False),
                "creator": member.get("creator"),
                "title": member.get("title"),
                "roles": member.get("roles"),
                "title_description": member.get("roles.title"),
                "role_description": member.get("roles.description"),
            }
            all_records.append(record)

        # For snapshot tables, return completed offset
        return iter(all_records), {"completed": True}

    def _read_teams(self, start_offset: dict) -> tuple[Iterator[dict], dict]:
        """
        Read teams from SignalFx API.

        Args:
            start_offset: Offset containing pagination info (not used for full refresh)

        Returns:
            Iterator of team records and next offset
        """
        all_records = []

        # Call the SignalFx teams API endpoint
        data = self._make_request("/v2/team")
        
        # The API returns a list of team objects
        teams = data if isinstance(data, list) else data.get("results", [])

        for team in teams:
            record = {
                "id": team.get("id"),
                "name": team.get("name"),
                "members": team.get("members"),
                "description": team.get("description"),
                "created": team.get("created"),
                "lastUpdated": team.get("lastUpdated"),
                "creator": team.get("creator"),
            }
            all_records.append(record)

        # For snapshot tables, return completed offset
        return iter(all_records), {"completed": True}

