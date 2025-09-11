# Databricks CLI MCP Server

A Model Context Protocol (MCP) server that enables LLMs to interact with Databricks through standardized MCP tools. The server acts as a bridge, translating MCP requests from LLMs into Databricks CLI commands and returning structured responses.

## Architecture

```
LLM ↔ MCP Client ↔ MCP Server ↔ Databricks CLI ↔ Databricks Platform
```

**How it works:**
1. **LLM** sends requests using standard MCP protocol tools
2. **MCP Server** receives MCP requests and translates them into Databricks CLI commands
3. **Databricks CLI** executes commands against the Databricks platform
4. **Results** flow back through the chain: CLI → MCP Server → MCP Client → LLM

## Key Benefits

- **LLM Integration**: LLMs use familiar MCP protocol without needing to understand CLI syntax
- **CLI-Powered Backend**: Leverages official Databricks CLI for robust, maintained functionality
- **Authentication Simplified**: Uses existing Databricks CLI profiles and configuration
- **Clean Separation**: Each component handles what it does best

## Features

- **MCP Protocol Compliance**: Implements standard MCP protocol for seamless LLM integration
- **Databricks CLI Integration**: All Databricks operations executed through official CLI
- **Tool Registration**: Exposes Databricks functionality as standardized MCP tools
- **Async Operations**: Built with asyncio for efficient concurrent operations
- **Profile Management**: Supports multiple Databricks CLI profiles and configurations

## Available MCP Tools

The server exposes these MCP tools that LLMs can use:

### Cluster Management
- **list_clusters**: List all Databricks clusters
- **create_cluster**: Create a new Databricks cluster
- **terminate_cluster**: Terminate a Databricks cluster
- **get_cluster**: Get information about a specific Databricks cluster
- **start_cluster**: Start a terminated Databricks cluster

### Job Management
- **list_jobs**: List all Databricks jobs
- **create_job**: Create a new Databricks job
- **run_job**: Run a Databricks job
- **get_job**: Get information about a specific job
- **delete_job**: Delete a Databricks job

### Notebook Management
- **list_notebooks**: List notebooks in a workspace directory
- **export_notebook**: Export a notebook from the workspace
- **import_notebook**: Import a notebook into the workspace
- **create_notebook**: Create a new notebook in the workspace
- **delete_notebook**: Delete a notebook from the workspace
- **run_notebook**: Execute a notebook and return results

### File System (DBFS)
- **list_files**: List files and directories in a DBFS path
- **upload_file**: Upload a file to DBFS
- **download_file**: Download a file from DBFS
- **delete_file**: Delete a file from DBFS

### SQL & Data
- **execute_sql**: Execute a SQL statement
- **create_table**: Create a table from data
- **query_table**: Query data from existing tables

### Model Management
- **register_model**: Register a model in MLflow Model Registry
- **deploy_model**: Deploy a model for serving
- **list_models**: List registered models

*Each tool accepts structured parameters via MCP and returns formatted JSON responses.*

## Prerequisites

### System Requirements
- **Python 3.10 or higher**
- **`uv` package manager** (recommended for MCP servers)
- **Databricks CLI** (required - must be installed and configured)

### Databricks CLI Installation

The MCP server requires the Databricks CLI to be installed and configured. The server will use the CLI to execute all Databricks operations on behalf of LLM requests:

#### Installation

```bash
# Using pip
pip install databricks-cli

# Using Homebrew (macOS)
brew install databricks/tap/databricks

# Using curl (Linux/macOS)
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

#### Configuration

Configure the Databricks CLI with your workspace using profiles (the MCP server will use this configuration):

```bash
# Configure databricks CLI with default profile
databricks configure --host <your-databricks-workspace-url>

# Or create named profiles for multiple workspaces
databricks configure --profile production --host <prod-workspace-url>
databricks configure --profile development --host <dev-workspace-url>
```

Verify your CLI setup (this ensures the MCP server can communicate with Databricks):

```bash
# Test the CLI connection with default profile
databricks clusters list

# Test with specific profile
databricks clusters list --profile production

# Check current configuration
databricks configure --help
```

## Installation

### Setup

1. Install `uv` if you don't have it already:

   ```bash
   # MacOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   
   # Windows (in PowerShell)
   irm https://astral.sh/uv/install.ps1 | iex
   ```

   Restart your terminal after installation.

2. Clone the repository:
   ```bash
   git clone https://github.com/manishsat/dbx-mcp-server.git
   cd dbx-mcp-server
   ```

3. Set up the project with `uv`:
   ```bash
   # Create and activate virtual environment
   uv venv
   
   # On Windows
   .\.venv\Scripts\activate
   
   # On Linux/Mac
   source .venv/bin/activate
   
   # Install dependencies in development mode
   uv pip install -e .
   
   # Install development dependencies
   uv pip install -e ".[dev]"
   ```

4. Verify Databricks CLI is configured:
   ```bash
   # Test CLI connection
   databricks clusters list
   
   # If using profiles, specify the profile
   databricks clusters list --profile myprofile
   ```

## Configuration

### MCP Server Configuration

The server uses Databricks CLI profiles for secure authentication. Configure the profile to use:

```bash
# Linux/Mac - Specify which CLI profile to use
export DATABRICKS_PROFILE=production

# Windows
set DATABRICKS_PROFILE=production
```

You can also create an `.env` file:

```env
# .env file
DATABRICKS_PROFILE=production
```

**Security Note**: This server only supports CLI profile-based authentication. Environment variables for `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are intentionally not supported to maintain security best practices.

## Running the MCP Server

Start the MCP server to enable LLM interactions with Databricks:

```bash
# Windows
.\start_mcp_server.ps1

# Linux/Mac
./start_mcp_server.sh
```

These wrapper scripts will start the MCP server, which will:
1. Initialize the MCP protocol listener
2. Register all Databricks tools
3. Wait for LLM requests via MCP clients
4. Execute Databricks CLI commands as needed

You can also run the server directly:

```bash
# Windows
.\scripts\start_mcp_server.ps1

# Linux/Mac
./scripts/start_mcp_server.sh
```

Once running, LLMs can connect to the server via MCP clients and use the registered Databricks tools.

## How LLMs Interact

1. **LLM Request**: LLM sends MCP tool request (e.g., "list_clusters")
2. **MCP Translation**: Server receives request and validates parameters
3. **CLI Execution**: Server runs corresponding Databricks CLI command
4. **Response Processing**: Server parses CLI output into structured JSON
5. **MCP Response**: Server returns formatted response to LLM via MCP

### Example Flow
```
LLM Request: Use tool "create_cluster" with name="test-cluster"
    ↓
MCP Server: Validates parameters, constructs CLI command
    ↓
CLI Command: databricks clusters create --cluster-name test-cluster ...
    ↓
CLI Output: JSON response with cluster details
    ↓
MCP Response: Structured JSON returned to LLM
```

## Project Structure

```
dbx-mcp-server/
├── src/                             # Source code
│   ├── __init__.py                  # Makes src a package
│   ├── __main__.py                  # Main entry point for the package
│   ├── main.py                      # Entry point for the MCP server
│   ├── cli/                         # Databricks CLI integration
│   │   ├── __init__.py              # Makes cli a package
│   │   ├── clusters.py              # Cluster CLI operations
│   │   ├── dbfs.py                  # DBFS CLI operations
│   │   ├── jobs.py                  # Jobs CLI operations
│   │   ├── notebooks.py             # Notebooks CLI operations
│   │   └── sql.py                   # SQL CLI operations
│   ├── core/                        # Core functionality
│   │   ├── __init__.py              # Makes core a package
│   │   ├── config.py                # Configuration management
│   │   └── utils.py                 # Utility functions
│   └── server/                      # Server implementation
│       ├── __init__.py              # Makes server a package
│       └── databricks_mcp_server.py # Main MCP server
├── tests/                           # Test directory
├── scripts/                         # Helper scripts
│   ├── start_mcp_server.ps1         # Server startup script (Windows)
│   ├── start_mcp_server.sh          # Server startup script (Linux/Mac)
│   ├── run_tests.ps1                # Test runner script
│   ├── show_clusters.py             # Script to show clusters
│   └── show_notebooks.py            # Script to show notebooks
├── examples/                        # Example usage
├── docs/                            # Documentation
└── pyproject.toml                   # Project configuration
```

## Development

### Code Standards

- Python code follows PEP 8 style guide with a maximum line length of 100 characters
- Use 4 spaces for indentation (no tabs)
- Use double quotes for strings
- All classes, methods, and functions should have Google-style docstrings
- Type hints are required for all code except tests

### Linting

The project uses the following linting tools:

```bash
# Run all linters
uv run pylint src/ tests/
uv run flake8 src/ tests/
uv run mypy src/
```

## Testing

The project uses pytest for testing. To run the tests:

```bash
# Run all tests with our convenient script
.\scripts\run_tests.ps1

# Run with coverage report
.\scripts\run_tests.ps1 -Coverage

# Run specific tests with verbose output
.\scripts\run_tests.ps1 -Verbose -Coverage tests/test_clusters.py
```

You can also run the tests directly with pytest:

```bash
# Run all tests
uv run pytest tests/

# Run with coverage report
uv run pytest --cov=src tests/ --cov-report=term-missing
```

A minimum code coverage of 80% is the goal for the project.

## Documentation

- API documentation is generated using Sphinx and can be found in the `docs/api` directory
- All code includes Google-style docstrings
- See the `examples/` directory for usage examples

## Examples

Check the `examples/` directory for usage examples. To run examples:

```bash
# Run example scripts with uv
uv run examples/direct_usage.py
uv run examples/mcp_client_usage.py
```

## Troubleshooting

### Common Issues

1. **Databricks CLI not found**
   ```bash
   # Verify installation
   databricks --version
   
   # Install if missing
   pip install databricks-cli
   ```

2. **Authentication Issues**
   ```bash
   # Reconfigure CLI
   databricks configure
   
   # Test connection
   databricks clusters list
   ```

3. **Profile Issues**
   ```bash
   # List profiles
   cat ~/.databrickscfg
   
   # Use specific profile
   export DATABRICKS_PROFILE=myprofile
   ```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Ensure your code follows the project's coding standards
2. Add tests for any new functionality
3. Update documentation as necessary
4. Verify all tests pass before submitting
5. Ensure Databricks CLI compatibility

## License

This project is licensed under the MIT License - see the LICENSE file for details.