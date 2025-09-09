# Databricks MCP Server

A Model Context Protocol (MCP) server that enables creating and managing Databricks jobs directly from Visual Studio Code and other AI-powered development environments.

## Features

- Create Databricks jobs with custom configurations
- Manage job parameters and clusters
- Monitor job execution status
- Integration with Visual Studio Code through MCP protocol

## Installation

```bash
pip install -e .
```

## Configuration

Create a `.env` file with your Databricks configuration:

```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-access-token
```

## Usage

Start the MCP server:

```bash
dbx-mcp-server
```

## MCP Tools Available

- `create_job`: Create a new Databricks job
- `list_jobs`: List existing jobs in the workspace
- `get_job`: Get details of a specific job
- `run_job`: Trigger a job run

## Development

Install development dependencies:

```bash
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

Format code:

```bash
black src/
isort src/
```