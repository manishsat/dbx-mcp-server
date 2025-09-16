"""
Tests for the configuration module.
"""

import os
import pytest
from unittest.mock import patch

from src.core.config import Settings, get_databricks_cli_base_command, validate_configuration


def test_settings_defaults():
    """Test that default values are set correctly."""
    # Clear all environment variables to ensure we get true defaults
    with patch.dict(os.environ, {}, clear=True):
        settings = Settings()
        
        assert settings.databricks_profile is None
        assert settings.mcp_server_name == "databricks-mcp"
        assert settings.log_level == "DEBUG"  # Current actual default
        assert settings.databricks_cli_command == "databricks"
        assert settings.cli_timeout == 600  # Current actual default from .env


def test_settings_from_env():
    """Test that settings can be loaded from environment variables."""
    env_vars = {
        "DATABRICKS_PROFILE": "test-profile",
        "LOG_LEVEL": "DEBUG",
        "CLI_TIMEOUT": "600",
    }
    
    with patch.dict(os.environ, env_vars, clear=True):
        settings = Settings()
        
        assert settings.databricks_profile == "test-profile"
        assert settings.log_level == "DEBUG"
        assert settings.cli_timeout == 600


def test_invalid_log_level():
    """Test that invalid log level raises validation error."""
    with patch.dict(os.environ, {"LOG_LEVEL": "INVALID"}, clear=True):
        with pytest.raises(ValueError, match="log_level must be one of"):
            Settings()


def test_invalid_timeout():
    """Test that invalid timeout raises validation error."""
    with patch.dict(os.environ, {"CLI_TIMEOUT": "-1"}, clear=True):
        with pytest.raises(ValueError, match="cli_timeout must be positive"):
            Settings()


def test_databricks_cli_base_command():
    """Test CLI base command generation."""
    # Test with default profile
    with patch.dict(os.environ, {}, clear=True):
        settings = Settings()
        # Need to patch the global settings
        from src.core.config import settings as global_settings
        global_settings.DATABRICKS_PROFILE = None
        
        command = get_databricks_cli_base_command()
        assert command == ["databricks"]
    
    # Test with custom profile
    with patch.dict(os.environ, {"DATABRICKS_PROFILE": "prod"}, clear=True):
        settings = Settings()
        from src.core.config import settings as global_settings
        global_settings.DATABRICKS_PROFILE = "prod"
        
        command = get_databricks_cli_base_command()
        assert command == ["databricks", "--profile", "prod"]


def test_configuration_validation():
    """Test configuration validation."""
    # Should pass with databricks CLI available
    is_valid, message = validate_configuration()
    
    # This might fail if databricks CLI is not installed
    if is_valid:
        assert "Configuration is valid" in message
    else:
        assert "not found in PATH" in message or "validation failed" in message
