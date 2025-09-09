# Example: Creating a Databricks Job with MCP Server

This example demonstrates how to use the Databricks MCP Server to create and manage Databricks jobs from Visual Studio Code.

## Prerequisites

1. Install the Databricks MCP Server:
   ```bash
   pip install -e .
   ```

2. Configure your Databricks credentials in `.env`:
   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-personal-access-token
   ```

3. Configure Claude Desktop with the MCP server (see `claude_desktop_config.json`)

## Example Interactions

### 1. List Available Clusters
Ask Claude: "Can you list the available Databricks clusters?"

The MCP server will call the `list_clusters` tool and return information about all clusters in your workspace.

### 2. Create a New Job
Ask Claude: "Create a Databricks job named 'Data Processing Pipeline' that runs the notebook at '/Shared/data-processing' on cluster 'my-cluster-id'"

This will use the `create_job` tool with the following parameters:
- name: "Data Processing Pipeline"
- notebook_path: "/Shared/data-processing"
- cluster_id: "my-cluster-id"

### 3. List All Jobs
Ask Claude: "Show me all the jobs in my Databricks workspace"

This will use the `list_jobs` tool to retrieve and display all jobs.

### 4. Run a Job
Ask Claude: "Run job ID 12345 with parameter 'date' set to '2024-01-15'"

This will use the `run_job` tool to trigger the job with the specified parameters.

## Sample Job Configuration

Here's an example of creating a job programmatically:

```python
from dbx_mcp_server.databricks_client import DatabricksClient

client = DatabricksClient()

job_id = await client.create_job(
    name="Daily ETL Pipeline",
    notebook_path="/Shared/etl/daily_pipeline",
    cluster_id="0123-456789-abc123",
    parameters={
        "input_path": "/mnt/raw-data",
        "output_path": "/mnt/processed-data",
        "date": "2024-01-15"
    }
)

print(f"Created job with ID: {job_id}")
```

## Troubleshooting

### Authentication Issues
- Ensure your `DATABRICKS_TOKEN` is valid and has the necessary permissions
- Verify your `DATABRICKS_HOST` URL is correct

### Permission Issues
- Make sure your token has permissions to create and manage jobs
- Check that you have access to the specified notebook paths and clusters

### Connection Issues
- Verify network connectivity to your Databricks workspace
- Check if your organization has any firewall restrictions