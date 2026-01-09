# Lakeflow Splunk Community Connector

This documentation describes how to configure and use the **Splunk** Lakeflow community connector to ingest organizational, dashboard, and metrics metadata from the Splunk REST API into Databricks.

The Lakeflow Splunk Connector allows you to extract member, team, dashboard, and metrics catalog metadata from your Splunk Enterprise or Splunk Cloud instance and load it into your data lake or warehouse. This connector provides visibility into your Splunk organizational structure, dashboard configurations, and metrics infrastructure, enabling governance, auditing, and analytics on your Splunk deployment.

**Extensibility**: This connector is designed to be easily extended. Additional data APIs (apps, saved searches, reports, indexes, etc.) can be added following the same pattern.

# Prerequisites

- **Splunk account**: Access to a Splunk Enterprise or Splunk Cloud instance with the REST API enabled
- **Splunk API token**: A valid API token with appropriate permissions
  - Required Splunk capabilities:
    - `list_users` - For reading member/user information
    - `admin_all_objects` or specific permissions for reading dashboards and teams
    - `list_metrics_catalog` - For reading metrics catalog data
- **Network access**: The environment running the connector must be able to reach your Splunk REST API endpoint
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines

## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the connection options exposed by the connector.

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `api_token` | string | Yes | SignalFx API token for authentication. Store securely using Databricks Secrets. | `eyJraWQ...` |
| `base_url` | string | Yes | Full base URL for the SignalFx API. | `https://api.us1.signalfx.com/` |

> **Note**: This connector uses token-based authentication for secure API access. The token should have the appropriate capabilities assigned (`list_users`, `admin_all_objects`, `list_metrics_catalog`).

> **Note**: This connector does not currently require `externalOptionsAllowList` as it does not use table-specific options. All configuration is at the connection level.

### Obtaining the Required Parameters

1. **Generate a SignalFx API Token**
   - Log in to your SignalFx account
   - Navigate to **Settings** → **Organization Settings** → **Access Tokens**
   - Click **New Token** and provide:
     - Token name (e.g., "Databricks Lakeflow Connector")
     - Select appropriate permissions/scopes for API access
   - Ensure the token has the required permissions:
     - Read access to members/users
     - Read access to teams
     - Read access to dashboards
     - Read access to metrics catalog
   - **Important**: Copy and save the token immediately - it will only be displayed once!

2. **Determine Your Base URL**
   - Use the SignalFx API URL for your realm: `https://api.{REALM}.signalfx.com/`
   - Common realms:
     - **US1** (US East): `https://api.us1.signalfx.com/`
     - **US2** (US West): `https://api.us2.signalfx.com/`
     - **EU0** (Europe): `https://api.eu0.signalfx.com/`
     - **JP0** (Japan): `https://api.jp0.signalfx.com/`
   - Check your SignalFx organization settings to confirm your realm

3. **Verify API Access**
   - Test connectivity using Postman:
     - **Method**: GET
     - **URL**: `https://api.{REALM}.signalfx.com/v2/organization`
     - **Authorization**: Custom Header
       - Header: `X-SF-TOKEN`
       - Value: `your-api-token`
   - If successful, you should receive a JSON response with organization information

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in multiple ways:

**Via Databricks UI:**
1. Follow the **Lakeflow Community Connector** UI flow from the **"Add Data"** page
2. Or navigate to **Catalog → Connections** and create a new **"Lakeflow Community Connector"** connection
3. Fill in the connection parameters listed above (`api_token`, `base_url`)
4. Upload or reference the connector source code from this repository

**Via Unity Catalog API:**
The connection can also be created programmatically using the standard Unity Catalog API or SQL:
```sql
CREATE CONNECTION my_signalfx_connection
TYPE GENERIC_LAKEFLOW_CONNECT
OPTIONS (
  api_token = 'eyJraWQ...your-token...',
  base_url = 'https://api.us1.signalfx.com/'
);
```

**Using Databricks Secrets (Recommended for Production):**
```sql
CREATE CONNECTION my_signalfx_connection
TYPE GENERIC_LAKEFLOW_CONNECT
OPTIONS (
  api_token = secret('my_scope', 'signalfx_api_token'),
  base_url = 'https://api.us1.signalfx.com/'
);
```


## Supported Objects

The Splunk connector exposes a **static list** of tables that provide organizational, dashboard, and metrics metadata from your Splunk deployment:

- `members` - User/member information
- `teams` - Team and role information
- `dashboards` - Dashboard definitions and metadata
- `metrics` - Metrics catalog with dimensions and metadata

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key for each table:

| Table | Description | Ingestion Type | Primary Key | Incremental Cursor |
|-------|-------------|----------------|-------------|-------------------|
| `members` | Splunk users with their roles, capabilities, and settings | `snapshot` | `username` | n/a |
| `teams` | Splunk roles (teams) with assigned capabilities and users | `snapshot` | `role_name` | n/a |
| `dashboards` | Dashboard definitions, panels, and configurations | `snapshot` | `dashboard_id` | n/a |
| `metrics` | Metric names with associated indexes and dimensions | `snapshot` | `metric_name` | n/a |

**Note**: All objects use **snapshot ingestion** because the Splunk REST API endpoints do not provide reliable change tracking. Each sync performs a full refresh. This is typically acceptable since this metadata changes infrequently.

**Extensibility**: Additional tables can be easily added by implementing new methods following the same pattern:
- `dimensions` - Dimension names from metrics catalog
- `rollup_policies` - Metric rollup aggregation policies
- `saved_searches` - Saved searches and reports
- `apps` - Installed apps and add-ons
- `indexes` - Index configurations
- `data_inputs` - Data input configurations
- `alerts` - Alert configurations
- `lookup_tables` - Lookup table definitions

### Table Details and Schemas

#### `members`
Retrieves all Splunk users (members) with their authentication details, assigned roles, capabilities, and account settings.

- **API Endpoint**: `GET /services/authentication/users`
- **Primary Key**: `username` (STRING)
- **Ingestion Type**: Snapshot (full refresh)
- **Schema**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `username` | STRING | No | Unique username/login for the Splunk user |
| `realname` | STRING | Yes | Full name of the user |
| `email` | STRING | Yes | Email address associated with the account |
| `type` | STRING | Yes | Authentication type (e.g., `Splunk`, `LDAP`, `SAML`) |
| `roles` | ARRAY<STRING> | Yes | List of roles assigned to this user |
| `capabilities` | ARRAY<STRING> | Yes | Effective capabilities from assigned roles |
| `default_app` | STRING | Yes | Default app that loads when user logs in |
| `tz` | STRING | Yes | User's timezone setting |
| `locked_out` | BOOLEAN | Yes | Whether the account is currently locked |
| `created_time` | STRING | Yes | Account creation timestamp |
| `last_successful_login` | STRING | Yes | Timestamp of last successful login |

**Use Cases**: 
- **User Access Auditing**: Track who has access to your Splunk environment and what capabilities they have
- **Security Compliance**: Monitor locked accounts, authentication types, and login activity
- **License Management**: Count active users for license compliance
- **Role Analysis**: Understand which users have admin privileges or sensitive capabilities

#### `teams`
Retrieves Splunk roles (representing teams) with their assigned capabilities, restrictions, and member counts.

- **API Endpoint**: `GET /services/authorization/roles`
- **Primary Key**: `role_name` (STRING)
- **Ingestion Type**: Snapshot (full refresh)
- **Schema**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `role_name` | STRING | No | Unique name of the role/team |
| `description` | STRING | Yes | Human-readable description of the role |
| `capabilities` | ARRAY<STRING> | Yes | List of capabilities granted by this role |
| `imported_roles` | ARRAY<STRING> | Yes | Other roles inherited by this role |
| `default_app` | STRING | Yes | Default app for users with this role |
| `search_filter` | STRING | Yes | Search restrictions applied to this role |
| `imported_capabilities` | ARRAY<STRING> | Yes | Capabilities inherited from imported roles |
| `srchIndexesAllowed` | ARRAY<STRING> | Yes | Indexes this role is allowed to search |
| `srchIndexesDefault` | ARRAY<STRING> | Yes | Indexes searched by default |
| `member_count` | INTEGER | Yes | Number of users assigned to this role |

**Use Cases**:
- **Permission Governance**: Understand what capabilities each role has
- **Role Hierarchy Analysis**: Map role inheritance and capability propagation
- **Security Auditing**: Identify roles with admin or sensitive capabilities
- **Index Access Control**: Track which teams can access which data indexes
- **Team Size Tracking**: Monitor how many users are in each role/team

#### `dashboards`
Retrieves dashboard definitions including panels, visualizations, search queries, and configurations.

- **API Endpoint**: `GET /services/data/ui/views`
- **Primary Key**: `dashboard_id` (STRING, combination of app + dashboard name)
- **Ingestion Type**: Snapshot (full refresh)
- **Schema**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `dashboard_id` | STRING | No | Unique identifier (format: `app:dashboard_name`) |
| `dashboard_name` | STRING | No | Name of the dashboard |
| `app` | STRING | No | Splunk app containing this dashboard |
| `label` | STRING | Yes | Display label for the dashboard |
| `description` | STRING | Yes | Dashboard description |
| `owner` | STRING | Yes | Username of the dashboard creator |
| `sharing` | STRING | Yes | Sharing level (`app`, `global`, `user`) |
| `is_visible` | BOOLEAN | Yes | Whether dashboard appears in navigation |
| `eai_data` | STRING | Yes | Dashboard XML/JSON definition |
| `panel_count` | INTEGER | Yes | Number of panels in the dashboard |
| `search_count` | INTEGER | Yes | Number of search queries used |
| `updated` | STRING | Yes | Last modification timestamp |
| `created` | STRING | Yes | Creation timestamp |
| `permissions` | STRUCT<read: ARRAY<STRING>, write: ARRAY<STRING>> | Yes | Read/write permissions |

**Use Cases**:
- **Dashboard Inventory**: Catalog all dashboards across your Splunk deployment
- **Dashboard Governance**: Track dashboard ownership, sharing, and permissions
- **Usage Analytics**: Identify popular dashboards and unused dashboards
- **Migration Planning**: Export dashboard definitions for migration or backup
- **Search Optimization**: Analyze search queries used in dashboards for performance tuning
- **Content Organization**: Understand dashboard distribution across apps and users

#### `metrics`
Retrieves all metric names available in the Splunk metrics catalog along with their associated indexes and dimensions.

- **API Endpoint**: `GET /services/catalog/metricstore/metrics`
- **Primary Key**: `metric_name` (STRING)
- **Ingestion Type**: Snapshot (full refresh)
- **Schema**:

| Field | Type | Nullable | Description |
|-------|------|----------|-------------|
| `metric_name` | STRING | No | Unique name of the metric (e.g., `cpu.utilization`, `memory.usage`) |
| `index` | STRING | Yes | Splunk index containing the metric data |
| `dimensions` | ARRAY<STRUCT<name: STRING, value: STRING>> | Yes | Associated dimension name-value pairs for this metric instance |

**Use Cases**:
- **Metrics Discovery**: Catalog all available metrics in your Splunk environment
- **Metrics Inventory**: Track which indexes contain which metrics
- **Dimension Analysis**: Understand what dimensions are used with each metric
- **Metrics Documentation**: Auto-generate documentation of available metrics
- **Data Lineage**: Map metrics to their source indexes
- **Metrics Governance**: Track metrics usage and standardization across teams


## Architecture

### How the Connector Works

The Splunk connector uses the Lakeflow Community Connector framework and consists of several components:

1. **LakeflowConnect Interface** (`sources/interface/lakeflow_connect.py`)
   - Defines the standard interface all connectors must implement
   - Methods: `list_tables()`, `get_table_schema()`, `read_table_metadata()`, `read_table()`

2. **Splunk Connector Implementation** (`sources/splunk/splunk.py`)
   - Implements the LakeflowConnect interface for Splunk
   - Handles authentication using HTTP Basic Auth
   - Makes REST API calls to Splunk Metrics Catalog endpoints
   - Performs pagination to retrieve all data

3. **Generated Python Source** (`_generated_splunk_python_source.py`)
   - Automatically generated by merging the connector with Lakeflow's datasource framework
   - Registered as a Spark datasource for seamless integration
   - Handles data type conversion from JSON to Spark types

4. **Ingestion Pipeline** (`pipeline/ingestion_pipeline.py`)
   - Orchestrates the data ingestion process
   - Creates Delta tables in Unity Catalog
   - Manages checkpointing and incremental loads (where applicable)

**Data Flow:**
```
Splunk REST API → HTTP Basic Auth → JSON/XML Response → 
Python Connector → Spark DataSource → Delta Table → Unity Catalog
```

### Example Use Cases

Once your Splunk organizational metadata is in Delta tables, you can:

1. **User Access Governance and Auditing**
   ```sql
   -- Find all users with admin capabilities
   SELECT username, realname, email, roles
   FROM splunk_members m
   WHERE EXISTS (
     SELECT 1 FROM splunk_teams t
     WHERE array_contains(m.roles, t.role_name)
       AND array_contains(t.capabilities, 'admin_all_objects')
   );
   
   -- Identify locked or inactive accounts
   SELECT username, realname, locked_out, last_successful_login
   FROM splunk_members
   WHERE locked_out = true OR last_successful_login < date_sub(current_date(), 90);
   ```

2. **Team and Permission Analysis**
   ```sql
   -- Analyze team sizes and capabilities
   SELECT 
     role_name,
     description,
     member_count,
     SIZE(capabilities) as capability_count,
     SIZE(srchIndexesAllowed) as accessible_indexes
   FROM splunk_teams
   ORDER BY member_count DESC;
   
   -- Find teams with sensitive capabilities
   SELECT role_name, capabilities
   FROM splunk_teams
   WHERE array_contains(capabilities, 'edit_user')
      OR array_contains(capabilities, 'delete_by_keyword');
   ```

3. **Dashboard Inventory and Governance**
   ```sql
   -- Dashboard inventory by app
   SELECT 
     app,
     COUNT(*) as dashboard_count,
     COUNT(DISTINCT owner) as unique_owners,
     AVG(panel_count) as avg_panels
   FROM splunk_dashboards
   GROUP BY app
   ORDER BY dashboard_count DESC;
   
   -- Find dashboards with complex visualizations
   SELECT dashboard_name, app, owner, panel_count, search_count
   FROM splunk_dashboards
   WHERE panel_count > 10 OR search_count > 20;
   ```

4. **User-Dashboard-Team Relationship Analysis**
   ```sql
   -- Find which teams have access to which dashboards
   SELECT 
     d.dashboard_name,
     d.app,
     d.owner,
     d.sharing,
     t.role_name,
     t.member_count
   FROM splunk_dashboards d
   CROSS JOIN splunk_teams t
   WHERE d.sharing = 'global' OR d.app = t.default_app;
   ```

5. **Security and Compliance Reporting**
   ```sql
   -- Compliance report: users, their roles, and last login
   SELECT 
     m.username,
     m.realname,
     m.email,
     m.type as auth_type,
     m.roles,
     m.last_successful_login,
     CASE 
       WHEN m.last_successful_login < date_sub(current_date(), 90) 
       THEN 'Inactive'
       ELSE 'Active'
     END as status
   FROM splunk_members m
   ORDER BY m.last_successful_login DESC;
   
   -- Index access audit
   SELECT 
     t.role_name,
     t.member_count,
     explode(t.srchIndexesAllowed) as allowed_index
   FROM splunk_teams t
   WHERE SIZE(t.srchIndexesAllowed) > 0;
   ```

6. **Dashboard Usage and Maintenance**
   - Identify orphaned dashboards (owner no longer exists)
   - Find dashboards that haven't been updated in 6+ months
   - Track dashboard distribution across teams for standardization
   - Analyze search complexity for performance optimization

7. **Metrics Catalog Analysis**
   ```sql
   -- Metrics inventory by index
   SELECT 
     index,
     COUNT(DISTINCT metric_name) as metric_count,
     COUNT(*) as total_metric_instances
   FROM splunk_metrics
   GROUP BY index
   ORDER BY metric_count DESC;
   
   -- Find metrics with the most dimensions
   SELECT 
     metric_name,
     index,
     SIZE(dimensions) as dimension_count,
     dimensions
   FROM splunk_metrics
   WHERE dimensions IS NOT NULL
   ORDER BY dimension_count DESC
   LIMIT 20;
   
   -- Identify dimension patterns across metrics
   SELECT 
     explode(dimensions).name as dimension_name,
     COUNT(DISTINCT metric_name) as metric_count
   FROM splunk_metrics
   WHERE dimensions IS NOT NULL
   GROUP BY dimension_name
   ORDER BY metric_count DESC;
   ```

## Data Type Mapping

The Splunk connector maps source data types to Databricks data types as follows:

| Splunk API Type | Databricks Type | Example Fields | Notes |
|-----------------|-----------------|----------------|-------|
| String | STRING | `username`, `role_name`, `dashboard_name`, `email` | Text fields |
| Integer | BIGINT | `member_count`, `panel_count`, `search_count` | Numeric values and counts |
| Boolean | BOOLEAN | `locked_out`, `is_visible` | True/false flags |
| Timestamp/Date String | STRING | `created_time`, `updated`, `last_successful_login` | ISO 8601 timestamps stored as strings |
| XML/JSON Object | STRUCT | `permissions` in dashboards | Nested objects with known structure |
| Array/List | ARRAY<STRING> | `roles`, `capabilities`, `srchIndexesAllowed` | String collections |
| Array of Objects | ARRAY<STRUCT> | Complex nested permissions or configurations | Nested object collections |
| XML Document | STRING | `eai_data` (dashboard XML definition) | Full XML stored as string for parsing |

**Special Field Behaviors:**
- **Role arrays**: Stored as native Spark ARRAY<STRING> for easy querying with `array_contains()`
- **Capabilities**: Stored as ARRAY<STRING> to enable capability-based filtering and analysis
- **Timestamps**: Stored as STRING in ISO 8601 format; cast to TIMESTAMP downstream if needed
- **Dashboard XML**: Full XML definition stored as STRING; parse with Spark XML functions if needed
- **Composite Keys**: `dashboard_id` combines `app` and `dashboard_name` for uniqueness
- **Null handling**: Missing or empty fields are represented as SQL `NULL`
- **Empty arrays**: Empty lists from API are stored as empty arrays `[]`, not `NULL`

### Extensibility: Adding More Data APIs

The connector is designed for easy extension. To add new Splunk data sources:

**Step 1: Add Table to `list_tables()`**
```python
def list_tables(self) -> list[str]:
    return [
        "members",
        "teams", 
        "dashboards",
        "saved_searches",  # New table
        "apps",            # New table
    ]
```

**Step 2: Define Schema in `get_table_schema()`**
```python
def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
    schemas = {
        # ... existing schemas ...
        "saved_searches": StructType([
            StructField("name", StringType(), False),
            StructField("search", StringType(), True),
            StructField("app", StringType(), True),
            # ... more fields ...
        ])
    }
    return schemas[table_name]
```

**Step 3: Add Metadata in `read_table_metadata()`**
```python
def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
    metadata = {
        # ... existing metadata ...
        "saved_searches": {
            "primary_keys": ["name", "app"],
            "ingestion_type": "snapshot",
        }
    }
    return metadata[table_name]
```

**Step 4: Implement Data Retrieval in `read_table()`**
```python
def read_table(self, table_name: str, start_offset: dict, table_options: dict[str, str]):
    if table_name == "saved_searches":
        return self._read_saved_searches(start_offset)
    # ... existing table handlers ...

def _read_saved_searches(self, start_offset: dict):
    # Make API call to /services/saved/searches
    # Parse response and yield records
    # Follow same pattern as existing methods
    pass
```

**Suggested Additional Tables:**
- `saved_searches` - Saved searches and reports (API: `/services/saved/searches`)
- `apps` - Installed Splunk apps (API: `/services/apps/local`)
- `indexes` - Index configurations (API: `/services/data/indexes`)
- `data_inputs` - Data input configurations (API: `/services/data/inputs/*`)
- `alerts` - Alert configurations (API: `/services/saved/searches?is_scheduled=1`)
- `macros` - Search macros (API: `/services/admin/macros`)
- `metrics` - Metrics catalog (API: `/services/catalog/metricstore/metrics`)
- `lookup_tables` - Lookup table definitions (API: `/services/data/lookup-table-files`)


## How to Run

### Overview: The Generated Pipeline

When you create a Lakeflow Community Connector pipeline for Splunk, the system automatically generates:

1. **`ingest.py`** - The main notebook that defines your pipeline specification and runs the ingestion
2. **Connector source code** - Merged Python source that includes the Splunk connector logic and Lakeflow interface
3. **Pipeline configuration** - Unity Catalog connection settings and table mappings

The generated `ingest.py` notebook contains a **pipeline specification** that defines:
- Which Unity Catalog connection to use (containing your Splunk credentials)
- Which tables to ingest (`metrics`, `dimensions`, `rollup_policies`)
- Where to store the data (catalog, schema, table names)
- Ingestion behavior (SCD type, primary keys, etc.)

### Step 1: Create the Unity Catalog Connection

Before running the pipeline, ensure you have created a Unity Catalog connection (see "Create a Unity Catalog Connection" section above). The connection stores your Splunk credentials securely.

### Step 2: Configure Your Pipeline Specification

The generated `ingest.py` contains a `pipeline_spec` dictionary that you can customize:

```python
from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "splunk"

# Define which tables to ingest and where to store them
pipeline_spec = {
    "connection_name": "my_splunk_connection",  # Your UC connection name
    "objects": [
        # Ingest members (users) table
        {
            "table": {
                "source_table": "members",
                "destination_catalog": "main",  # Optional: defaults to workspace default
                "destination_schema": "splunk_governance",  # Optional: defaults to workspace default
                "destination_table": "splunk_members",  # Optional: defaults to source_table name
                "table_configuration": {
                    "scd_type": "SCD_TYPE_1",  # Full refresh (default for snapshot tables)
                }
            }
        },
        # Ingest teams (roles) table
        {
            "table": {
                "source_table": "teams",
                "destination_table": "splunk_teams"
            }
        },
        # Ingest dashboards table
        {
            "table": {
                "source_table": "dashboards",
                "destination_table": "splunk_dashboards"
            }
        },
        # Ingest metrics catalog table
        {
            "table": {
                "source_table": "metrics",
                "destination_table": "splunk_metrics"
            }
        }
    ]
}

# Register and run the pipeline
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)
ingest(spark, pipeline_spec)
```

**Pipeline Spec Parameters:**
- `connection_name` (required): Name of the Unity Catalog connection containing Splunk credentials
- `objects` (required): List of tables to ingest
  - `source_table` (required): Table name from the connector (`members`, `teams`, `dashboards`, or `metrics`)
  - `destination_catalog` (optional): Target catalog in Unity Catalog
  - `destination_schema` (optional): Target schema in Unity Catalog
  - `destination_table` (optional): Target table name (defaults to `source_table`)
  - `table_configuration` (optional):
    - `scd_type`: Ingestion mode - use `"SCD_TYPE_1"` for snapshot tables (default)
    - `primary_keys`: Override the default primary keys if needed

**Adding More Tables**: To extend the connector with additional Splunk data:
1. Add new table name to `list_tables()` in `splunk.py`
2. Define schema in `get_table_schema()`
3. Add metadata in `read_table_metadata()`
4. Implement data retrieval in `read_table()` following the same REST API pattern
5. Update the pipeline spec to include the new table

See the "Extensibility" section below for suggested additional tables like `dimensions`, `rollup_policies`, `saved_searches`, `apps`, etc.

### Step 3: Run the Pipeline

**Option 1: Run the Generated Notebook**
1. Open the generated `ingest.py` notebook in your Databricks workspace
2. Attach to a cluster (requires DBR 13.0+ with Python 3.10+)
3. Click **"Run All"** to execute the pipeline

**Option 2: Schedule the Pipeline**
1. Navigate to **Workflows** in your Databricks workspace
2. Create a new **Job** that runs the `ingest.py` notebook
3. Set a schedule (e.g., daily at 2 AM) based on how frequently your metrics catalog changes
4. Configure alerts for failures

**Option 3: Use Delta Live Tables (DLT)**
You can also integrate the connector into a Delta Live Tables pipeline for more sophisticated data quality and orchestration capabilities.

### Step 4: Monitor and Verify

After running the pipeline, verify the data was ingested successfully:

```sql
-- Check that tables were created
SHOW TABLES IN main.splunk_governance;

-- Verify members were loaded
SELECT COUNT(*) as user_count FROM main.splunk_governance.splunk_members;
SELECT username, realname, email, roles FROM main.splunk_governance.splunk_members LIMIT 10;

-- Verify teams were loaded
SELECT COUNT(*) as team_count FROM main.splunk_governance.splunk_teams;
SELECT role_name, description, member_count, capabilities FROM main.splunk_governance.splunk_teams LIMIT 10;

-- Verify dashboards were loaded
SELECT COUNT(*) as dashboard_count FROM main.splunk_governance.splunk_dashboards;
SELECT dashboard_name, app, owner, panel_count FROM main.splunk_governance.splunk_dashboards LIMIT 10;

-- Verify metrics were loaded
SELECT COUNT(*) as metric_count FROM main.splunk_governance.splunk_metrics;
SELECT metric_name, index, dimensions FROM main.splunk_governance.splunk_metrics LIMIT 10;
```

### Best Practices

- **Start Small**: Begin by syncing a single object (e.g., `members` or `metrics`) to test connectivity before ingesting all tables
- **Test Credentials First**: Use the Postman requests from the setup section to verify API access before creating the pipeline
- **Full Refresh Behavior**: This connector uses snapshot ingestion, so each run retrieves all data. This is suitable for metadata which changes infrequently.
- **Set Appropriate Schedules**: 
  - For most environments, daily syncs are sufficient for members, teams, and metrics
  - Dashboards may benefit from more frequent syncs (every 6-12 hours) if your team actively develops dashboards
  - Schedule during off-peak hours if you have large datasets (1000+ users, 100+ dashboards, or 1000+ metrics)
- **Token Security**: 
  - **Always use Databricks Secrets** to store API tokens instead of hardcoding them
  - Rotate tokens regularly for security
  - Set token expiration times in Splunk for additional security
- **SSL/TLS**: 
  - Always use `https://` in your `base_url`
  - Ensure valid SSL certificates for production environments
- **Monitor Pipeline Health**: Set up alerts in Databricks Workflows to notify you of pipeline failures
- **Data Quality**: Add data quality checks to ensure metrics catalog data meets your expectations

### Troubleshooting

#### Common Issues

**Authentication Errors (401)**
- **Symptom**: `Splunk API authentication failed. Check API token.`
- **Solutions**:
  - Verify the API token is correct in your Unity Catalog connection
  - Ensure the token hasn't expired (check token expiration in Splunk)
  - Confirm the token is still active (not revoked) in Splunk
  - Verify the token user/service account is active and not locked
  - Test authentication using Postman with Bearer Token authorization

**Access Forbidden (403)**
- **Symptom**: `Splunk API access forbidden. Ensure user has required capabilities`
- **Solutions**:
  - The user lacks required capabilities (`list_users`, `admin_all_objects`, `list_metrics_catalog`, etc.)
  - Log into Splunk as admin → **Settings** → **Access controls** → **Roles**
  - Edit the user's role and add the necessary capabilities:
    - `list_users` - For reading member information
    - `admin_all_objects` or specific permissions for dashboards
    - `list_metrics_catalog` - For reading metrics catalog data
  - Contact your Splunk administrator if you don't have permission to modify roles
  - Different tables require different capabilities - verify the user has all needed permissions

**Connection Errors / Timeouts**
- **Symptom**: `Connection refused` or `Timeout` errors
- **Solutions**:
  - Verify the `base_url` parameter is correct: `https://api.{REALM}.signalfx.com/`
  - Confirm your SignalFx realm (US1, US2, EU0, JP0)
  - Ensure network connectivity from Databricks to SignalFx API endpoint
  - Check firewall rules allow traffic from your Databricks workspace IPs
  - Verify your SignalFx realm URL is correct
  - Test connectivity using Postman:
    - **Method**: GET
    - **URL**: `https://api.{REALM}.signalfx.com/v2/organization`
    - **Header**: `X-SF-TOKEN: your-api-token`

**SSL Certificate Errors**
- **Symptom**: SSL verification failures
- **Solutions**:
  - Ensure the `base_url` uses `https://` protocol
  - For self-signed certificates, you may need to configure your Databricks workspace to trust the certificate
  - For Splunk Cloud, SSL should work by default with valid certificates
  - Verify the certificate is not expired

**Empty Results / No Data**
- **Symptom**: Tables created but contain 0 rows
- **Solutions**:
  - **For members table**: Verify user has `list_users` capability
  - **For teams table**: Verify user has permissions to view roles (`list_auth`)
  - **For dashboards table**: Verify user has `admin_all_objects` or specific dashboard permissions
  - **For metrics table**: Verify user has `list_metrics_catalog` capability and that metrics have been ingested into Splunk
  - Check if your Splunk instance is using external authentication (LDAP/SAML) - some users may not appear in local user list
  - For dashboards, ensure you're looking in the right apps (some dashboards are app-specific)
  - For metrics, confirm metrics data exists in Splunk UI → **Settings** → **Metrics Catalog**
  - Try accessing the REST API manually using Postman to verify data exists:
    - **Method**: GET
    - **URL**: `https://host:8089/services/authentication/users?output_mode=json` (for members)
    - **URL**: `https://host:8089/services/catalog/metricstore/metrics?output_mode=json` (for metrics)
    - **Authorization**: Bearer Token

**TypeError: Object of type ellipsis is not JSON serializable**
- **Symptom**: Error when running the pipeline with `...` in the error message
- **Solutions**:
  - Remove any `...` (ellipsis) placeholders in your `pipeline_spec`
  - Ensure all configuration values are actual strings/numbers, not placeholders
  - Check that `table_configuration` is either a proper dict or omitted entirely

**Missing Connection Parameters**
- **Symptom**: `Missing required option: 'host'` or similar errors
- **Solutions**:
  - Ensure Unity Catalog connection was created successfully
  - Verify connection name in `pipeline_spec` matches your UC connection name
  - Run: `SHOW CONNECTIONS;` to list available connections
  - Recreate the connection with all required parameters

**Pipeline-Level Errors**
- **Symptom**: Errors in the ingestion pipeline itself
- **Solutions**:
  - Check Databricks cluster logs for detailed error messages
  - Ensure cluster is using DBR 13.0+ with Python 3.10+
  - Verify the connector source code was uploaded/referenced correctly
  - Check that the `connection_name` in `pipeline_spec` is correct

#### Getting Help

**Error Handling:**
The connector includes built-in error handling for common scenarios including authentication failures, network issues, and API errors. Check the pipeline logs for detailed error information and recommended actions.

**Debug Tips:**
1. Enable verbose logging in your notebook: `spark.sparkContext.setLogLevel("DEBUG")`
2. Test connection manually before running full pipeline using Postman:
   
   **Test Members Endpoint:**
   - **Method**: GET
   - **URL**: `https://host:8089/services/authentication/users?output_mode=json`
   - **Authorization**: Bearer Token
     - Token: `your-api-token`
   
   **Test Teams Endpoint:**
   - **Method**: GET
   - **URL**: `https://host:8089/services/authorization/roles?output_mode=json`
   - **Authorization**: Bearer Token
     - Token: `your-api-token`
   
   **Test Dashboards Endpoint:**
   - **Method**: GET
   - **URL**: `https://host:8089/services/data/ui/views?output_mode=json`
   - **Authorization**: Bearer Token
     - Token: `your-api-token`
   
   **Test Metrics Endpoint:**
   - **Method**: GET
   - **URL**: `https://host:8089/services/catalog/metricstore/metrics?output_mode=json`
   - **Authorization**: Bearer Token
     - Token: `your-api-token`
   
   **Postman Settings:**
   - Use **Bearer Token** in the Authorization tab
   - Add header: `Authorization: Bearer your-api-token`
   - Add `count=10` query parameter to limit results for testing

3. Start with one table (e.g., `members` or `metrics`) to isolate issues
4. Check Splunk's internal logs (`_internal` index) for API access attempts
5. Verify user capabilities in Splunk UI: **Settings** → **Access controls** → **Roles**

**Resources:**
- Connector source code: Check `_generated_splunk_python_source.py` for implementation details
- Splunk REST API Documentation:
  - [Authentication/Users API](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTaccess#authentication.2Fusers)
  - [Authorization/Roles API](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTaccess#authorization.2Froles)
  - [Views/Dashboards API](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTknowledge#data.2Fui.2Fviews)
  - [Metrics Catalog API](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTmetrics#catalog.2Fmetricstore.2Fmetrics)
- Databricks Lakeflow docs: https://docs.databricks.com/en/lakeflow/index.html


## References

### Splunk REST API Documentation
- [Splunk REST API Reference Manual](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF)
- [Splunk REST API Endpoints Reference List](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTlist)

### API Endpoints Used by This Connector
- [Authentication/Users Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTaccess#authentication.2Fusers) - For members table
- [Authorization/Roles Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTaccess#authorization.2Froles) - For teams table
- [Data/UI/Views Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTknowledge#data.2Fui.2Fviews) - For dashboards table
- [Metrics Catalog Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTmetrics#catalog.2Fmetricstore.2Fmetrics) - For metrics table

### Additional APIs for Extension
- [Metrics Dimensions Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTmetrics#catalog.2Fmetricstore.2Fdimensions) - For dimensions table
- [Metrics Rollup Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTmetrics#catalog.2Fmetricstore.2Frollup) - For rollup_policies table
- [Saved Searches Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTsearch#saved.2Fsearches) - For saved searches
- [Apps Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTapps#apps.2Flocal) - For apps
- [Indexes Endpoint](https://docs.splunk.com/Documentation/Splunk/latest/RESTREF/RESTindex#data.2Findexes) - For indexes

### Databricks Resources
- [Databricks Lakeflow Documentation](https://docs.databricks.com/en/lakeflow/index.html)
- [Unity Catalog Connections](https://docs.databricks.com/en/connect/unity-catalog/connections.html)
