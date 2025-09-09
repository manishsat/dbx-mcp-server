# Visual Studio Code Integration Guide

This guide explains how to integrate the Databricks MCP Server with Visual Studio Code to enable AI-powered Databricks job creation.

## Prerequisites

1. **Claude Desktop** - Download and install Claude Desktop from Anthropic
2. **Databricks Workspace** - Access to a Databricks workspace with permissions to create jobs
3. **Personal Access Token** - A Databricks personal access token

## Installation

### 1. Install the Databricks MCP Server

```bash
git clone https://github.com/manishsat/dbx-mcp-server.git
cd dbx-mcp-server
pip install -e .
```

### 2. Configure Environment Variables

Create a `.env` file in your project directory:

```bash
cp .env.example .env
# Edit .env with your Databricks credentials
```

### 3. Configure Claude Desktop

Edit your Claude Desktop configuration file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

Add the following configuration:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "python",
      "args": ["-m", "dbx_mcp_server.main"],
      "env": {
        "DATABRICKS_HOST": "https://your-workspace.cloud.databricks.com",
        "DATABRICKS_TOKEN": "your-token-here"
      }
    }
  }
}
```

### 4. Restart Claude Desktop

Close and restart Claude Desktop to load the new MCP server configuration.

## Usage Examples

### Creating a Databricks Job

You can now ask Claude to create Databricks jobs using natural language:

**Example 1: Basic Job Creation**
```
"Create a Databricks job named 'Daily ETL Pipeline' that runs the notebook at '/Shared/etl/daily_pipeline' on cluster 'production-cluster-001'"
```

**Example 2: Job with Parameters**
```
"Create a job called 'Data Processing' for notebook '/Users/me/process_data' with parameters: input_path=/data/raw, output_path=/data/processed, date=2024-01-15"
```

### Managing Existing Jobs

**List all jobs:**
```
"Show me all the jobs in my Databricks workspace"
```

**Get job details:**
```
"Get details for job ID 12345"
```

**Run a job:**
```
"Run job 12345 with parameter 'environment' set to 'production'"
```

### Cluster Management

**List clusters:**
```
"What clusters are available in my workspace?"
```

## Available MCP Tools

The server provides the following tools:

- `create_job` - Create a new Databricks job
- `list_jobs` - List all jobs in the workspace
- `get_job` - Get details of a specific job
- `run_job` - Trigger a job run
- `list_clusters` - List available clusters

## Troubleshooting

### Authentication Issues

If you see authentication errors:

1. Verify your `DATABRICKS_HOST` URL is correct
2. Ensure your `DATABRICKS_TOKEN` is valid and has sufficient permissions
3. Check that your token hasn't expired

### Connection Issues

1. Verify network connectivity to your Databricks workspace
2. Check if your organization has firewall restrictions
3. Ensure the MCP server is running correctly

### Claude Desktop Not Recognizing the Server

1. Check the configuration file syntax is valid JSON
2. Verify the file path in the configuration exists
3. Restart Claude Desktop after configuration changes
4. Check Claude Desktop logs for error messages

## Advanced Configuration

### Custom Cluster Configurations

You can specify default cluster settings in the job creation. The server will automatically use a default cluster configuration if none is specified.

### Environment-Specific Configurations

For different environments (dev, staging, prod), you can create separate configuration files and switch between them.

## Security Best Practices

1. **Never commit credentials** to version control
2. **Use environment variables** for sensitive configuration
3. **Rotate tokens regularly** according to your organization's security policy
4. **Use least-privilege access** - only grant necessary permissions to the token

## Support

For issues and questions:
1. Check the project README.md
2. Review the example configurations
3. Open an issue on the GitHub repository