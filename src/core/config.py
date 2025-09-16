"""
Configuration settings for the Databricks CLI MCP server.

This module handles configuration from environment variables, .env files,
and provides settings for the MCP server and Databricks CLI integration.
"""

import os
from typing import Optional

try:
    from dotenv import load_dotenv
    # Load .env file if it exists
    load_dotenv()
except ImportError:
    # dotenv is optional - we can work without it
    pass

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Version
VERSION = "0.1.0"


class Settings(BaseSettings):
    """Configuration settings for the Databricks CLI MCP server."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )
    
    # Databricks CLI Configuration
    DATABRICKS_PROFILE: Optional[str] = Field(
        default=None,
        description="Databricks CLI profile to use (defaults to default profile)"
    )
    
    # MCP Server Configuration
    MCP_SERVER_NAME: str = Field(
        default="databricks-mcp",
        description="Name of the MCP server"
    )
    
    MCP_SERVER_VERSION: str = Field(
        default=VERSION,
        description="Version of the MCP server"
    )
    
    # Logging Configuration
    LOG_LEVEL: str = Field(
        default="DEBUG",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    
    # CLI Command Configuration
    DATABRICKS_CLI_COMMAND: str = Field(
        default="databricks",
        description="Databricks CLI command (useful for custom installations)"
    )
    
    # Timeout settings
    CLI_TIMEOUT: int = Field(
        default=300,
        description="Timeout for CLI commands in seconds"
    )
    
    @field_validator("LOG_LEVEL")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()
    
    @field_validator("CLI_TIMEOUT")
    def validate_cli_timeout(cls, v: int) -> int:
        """Validate CLI timeout."""
        if v <= 0:
            raise ValueError("cli_timeout must be positive")
        return v
    
    # Properties for backward compatibility and easier access
    @property
    def databricks_profile(self) -> Optional[str]:
        return self.DATABRICKS_PROFILE
    
    @property 
    def mcp_server_name(self) -> str:
        return self.MCP_SERVER_NAME
    
    @property
    def mcp_server_version(self) -> str:
        return self.MCP_SERVER_VERSION
    
    @property
    def log_level(self) -> str:
        return self.LOG_LEVEL
    
    @property
    def databricks_cli_command(self) -> str:
        return self.DATABRICKS_CLI_COMMAND
    
    @property
    def cli_timeout(self) -> int:
        return self.CLI_TIMEOUT


# Create global settings instance
settings = Settings()


def get_databricks_cli_base_command() -> list[str]:
    """
    Get the base Databricks CLI command with profile if specified.
    
    Returns:
        List of command parts for subprocess execution
    """
    command_parts = [settings.databricks_cli_command]
    
    if settings.databricks_profile:
        command_parts.extend(["--profile", settings.databricks_profile])
    
    return command_parts


def get_databricks_profile_info() -> dict[str, Optional[str]]:
    """
    Get information about the current Databricks profile configuration.
    
    Returns:
        Dictionary with profile information
    """
    return {
        "profile": settings.databricks_profile or "default",
        "cli_command": settings.databricks_cli_command,
        "timeout": str(settings.cli_timeout),
    }


def validate_configuration() -> tuple[bool, str]:
    """
    Validate the current configuration.
    
    Returns:
        Tuple of (is_valid, message)
    """
    try:
        # Test basic settings validation
        settings.model_validate(settings.model_dump())
        
        # Check if databricks CLI command exists
        import shutil
        if not shutil.which(settings.databricks_cli_command):
            return False, f"Databricks CLI command '{settings.databricks_cli_command}' not found in PATH"
        
        return True, "Configuration is valid"
        
    except Exception as e:
        return False, f"Configuration validation failed: {str(e)}"
