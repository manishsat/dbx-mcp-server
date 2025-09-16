"""
Main entry point for running the databricks-mcp-server package.
This allows the package to be run with 'python -m src' or 'uv run src'.
"""

from src.main import cli_main

if __name__ == "__main__":
    cli_main()
