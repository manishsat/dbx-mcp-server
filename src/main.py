"""
Main entry point for the Databricks CLI MCP server.

This module provides the main function to start the MCP server
with proper configuration and logging setup.
"""

import asyncio
import sys

from src.core.config import settings, validate_configuration
from src.core.utils import setup_logging


async def main() -> int:
    """
    Main entry point for the MCP server.
    
    Returns:
        Exit code (0 for success, 1 for error)
    """
    # Set up logging first
    logger = setup_logging()
    
    # Log startup information
    logger.info(f"Starting Databricks CLI MCP Server v{settings.mcp_server_version}")
    logger.info(f"Server name: {settings.mcp_server_name}")
    logger.info(f"Log level: {settings.log_level}")
    
    # Validate configuration
    is_valid, message = validate_configuration()
    if not is_valid:
        logger.error(f"Configuration validation failed: {message}")
        return 1
    
    logger.info("Configuration validation passed")
    
    # Log Databricks configuration (without sensitive info)
    profile_info = {}
    if settings.databricks_profile:
        profile_info["profile"] = settings.databricks_profile
    else:
        profile_info["profile"] = "default"
    
    profile_info["cli_command"] = settings.databricks_cli_command
    profile_info["timeout"] = f"{settings.cli_timeout}s"
    
    logger.info(f"Databricks configuration: {profile_info}")
    
    try:
        # Import and start the MCP server
        from src.mcp_server import main as mcp_main
        logger.info("Starting MCP server...")
        await mcp_main()
        return 0
        
    except Exception as e:
        logger.error(f"Error starting MCP server: {str(e)}", exc_info=True)
        return 1


def cli_main() -> None:
    """
    CLI entry point that handles async main and exit codes.
    """
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
        sys.exit(0)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli_main()
