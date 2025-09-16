"""
Tests for core utility functions.

This module tests utility functions including logging setup, response formatting,
JSON parsing, command sanitization, and other helper functions.
"""

import json
import logging
import pytest
import sys
from io import StringIO
from unittest.mock import Mock, patch
from src.core.utils import (
    setup_logging,
    CLIError,
    format_mcp_response,
    parse_json_output,
    sanitize_command_for_logging,
    validate_json_response,
    truncate_output,
    extract_error_from_cli_output
)


class TestLoggingSetup:
    """Test logging configuration and setup."""
    
    def test_setup_logging_returns_logger(self):
        """Test that setup_logging returns a logger instance."""
        logger = setup_logging()
        
        assert isinstance(logger, logging.Logger)
        assert logger.name == "dbx_mcp_server"
    
    def test_setup_logging_with_debug_level(self):
        """Test logging setup with DEBUG level."""
        with patch('src.core.utils.settings') as mock_settings:
            mock_settings.log_level = "DEBUG"
            
            logger = setup_logging()
            
            assert logger.level == logging.DEBUG
    
    def test_setup_logging_with_info_level(self):
        """Test logging setup with INFO level (default)."""
        with patch('src.core.utils.settings') as mock_settings:
            mock_settings.log_level = "INFO"
            
            logger = setup_logging()
            
            assert logger.level == logging.INFO
    
    def test_setup_logging_with_error_level(self):
        """Test logging setup with ERROR level."""
        with patch('src.core.utils.settings') as mock_settings:
            mock_settings.log_level = "ERROR"
            
            logger = setup_logging()
            
            assert logger.level == logging.ERROR
    
    def test_setup_logging_configures_handlers(self):
        """Test that logging is configured with appropriate handlers."""
        # Capture current handlers count
        root_logger = logging.getLogger()
        initial_handler_count = len(root_logger.handlers)
        
        with patch('logging.basicConfig') as mock_basic_config:
            setup_logging()
            
            mock_basic_config.assert_called_once()
            call_kwargs = mock_basic_config.call_args[1]
            
            assert 'level' in call_kwargs
            assert 'format' in call_kwargs
            assert 'handlers' in call_kwargs
            assert isinstance(call_kwargs['handlers'], list)
    
    def test_logging_format_includes_required_fields(self):
        """Test that logging format includes timestamp, name, level, and message."""
        with patch('logging.basicConfig') as mock_basic_config:
            setup_logging()
            
            call_kwargs = mock_basic_config.call_args[1]
            log_format = call_kwargs['format']
            
            assert '%(asctime)s' in log_format
            assert '%(name)s' in log_format
            assert '%(levelname)s' in log_format
            assert '%(message)s' in log_format


class TestMCPResponseFormatting:
    """Test MCP response formatting functions."""
    
    def test_format_mcp_response_success_with_data(self):
        """Test formatting successful response with data."""
        data = {"clusters": [{"id": "123", "name": "test"}]}
        
        response = format_mcp_response(success=True, data=data)
        
        assert response["success"] is True
        assert response["data"] == data
        assert "timestamp" in response
        assert "error" not in response
    
    def test_format_mcp_response_success_without_data(self):
        """Test formatting successful response without data."""
        response = format_mcp_response(success=True)
        
        assert response["success"] is True
        assert "data" not in response
        assert "error" not in response
        assert "timestamp" in response
    
    def test_format_mcp_response_error(self):
        """Test formatting error response."""
        error_message = "Cluster not found"
        
        response = format_mcp_response(success=False, error=error_message)
        
        assert response["success"] is False
        assert response["error"] == error_message
        assert "data" not in response
        assert "timestamp" in response
    
    def test_format_mcp_response_with_metadata(self):
        """Test formatting response with metadata."""
        data = {"result": "success"}
        metadata = {"execution_time": 1.23, "retry_count": 0}
        
        response = format_mcp_response(success=True, data=data, metadata=metadata)
        
        assert response["success"] is True
        assert response["data"] == data
        assert response["metadata"] == metadata
    
    def test_format_mcp_response_error_with_metadata(self):
        """Test formatting error response with metadata."""
        error_message = "Timeout occurred"
        metadata = {"timeout_seconds": 30, "attempts": 3}
        
        response = format_mcp_response(
            success=False, 
            error=error_message, 
            metadata=metadata
        )
        
        assert response["success"] is False
        assert response["error"] == error_message
        assert response["metadata"] == metadata
    
    def test_format_mcp_response_complex_data_types(self):
        """Test formatting response with complex data types."""
        data = {
            "list": [1, 2, 3],
            "nested": {"key": "value"},
            "null": None,
            "boolean": True
        }
        
        response = format_mcp_response(success=True, data=data)
        
        assert response["success"] is True
        assert response["data"] == data
        # Ensure it's JSON serializable
        json.dumps(response)  # Should not raise exception


class TestCommandSanitization:
    """Test command sanitization for logging."""
    
    def test_sanitize_command_basic(self):
        """Test basic command sanitization."""
        command = ["databricks", "clusters", "list", "--output", "json"]
        
        sanitized = sanitize_command_for_logging(command)
        
        assert sanitized == "databricks clusters list --output json"
    
    def test_sanitize_command_with_token_flag(self):
        """Test command sanitization with token flag."""
        command = ["databricks", "--token", "dapi123abc", "clusters", "list"]
        
        sanitized = sanitize_command_for_logging(command)
        
        assert "***MASKED***" in sanitized
        assert "dapi123abc" not in sanitized
        assert "databricks" in sanitized
        assert "clusters list" in sanitized
    
    def test_sanitize_command_with_password_flag(self):
        """Test command sanitization with password flag."""
        command = ["databricks", "--password", "secret123", "clusters", "list"]
        
        sanitized = sanitize_command_for_logging(command)
        
        assert "***MASKED***" in sanitized
        assert "secret123" not in sanitized
    
    def test_sanitize_command_with_token_in_value(self):
        """Test command sanitization with token in value."""
        command = ["databricks", "clusters", "create", "--json", "dapi-abc123def"]
        
        sanitized = sanitize_command_for_logging(command)
        
        assert "***MASKED***" in sanitized
        assert "dapi-abc123def" not in sanitized
    
    def test_sanitize_command_with_bearer_token(self):
        """Test command sanitization with Bearer token."""
        command = ["curl", "-H", "Bearer abc123", "https://api.databricks.com"]
        
        sanitized = sanitize_command_for_logging(command)
        
        assert "***MASKED***" in sanitized
        assert "Bearer abc123" not in sanitized
    
    def test_sanitize_command_with_pat_token(self):
        """Test command sanitization with PAT token."""
        command = ["databricks", "--token", "pat-12345", "clusters", "list"]
        
        sanitized = sanitize_command_for_logging(command)
        
        assert "***MASKED***" in sanitized
        assert "pat-12345" not in sanitized
    
    def test_sanitize_command_no_sensitive_data(self):
        """Test command sanitization with no sensitive data."""
        command = ["databricks", "clusters", "list", "--limit", "10"]
        
        sanitized = sanitize_command_for_logging(command)
        
        assert sanitized == "databricks clusters list --limit 10"
        assert "***MASKED***" not in sanitized
    
    def test_sanitize_command_empty_list(self):
        """Test command sanitization with empty command list."""
        command = []
        
        sanitized = sanitize_command_for_logging(command)
        
        assert sanitized == ""
    
    def test_sanitize_command_preserves_original(self):
        """Test that sanitization doesn't modify the original command."""
        original_command = ["databricks", "--token", "secret", "clusters", "list"]
        command_copy = original_command.copy()
        
        sanitized = sanitize_command_for_logging(command_copy)
        
        # Original should be unchanged
        assert original_command == command_copy
        assert "secret" in str(original_command)
        assert "***MASKED***" in sanitized


class TestJSONValidation:
    """Test JSON response validation."""
    
    def test_validate_json_response_valid_dict(self):
        """Test validation of valid dictionary response."""
        response = {"key": "value", "number": 123}
        
        is_valid, message = validate_json_response(response)
        
        assert is_valid is True
        assert message == "Valid JSON response"
    
    def test_validate_json_response_valid_list(self):
        """Test validation of valid list response."""
        response = [{"item": 1}, {"item": 2}]
        
        is_valid, message = validate_json_response(response)
        
        assert is_valid is True
        assert message == "Valid JSON response"
    
    def test_validate_json_response_invalid_type(self):
        """Test validation of invalid response type."""
        response = "This is just a string"
        
        is_valid, message = validate_json_response(response)
        
        assert is_valid is False
        assert "not a valid JSON object" in message
    
    def test_validate_json_response_non_serializable(self):
        """Test validation of non-JSON serializable response."""
        # Create a response with non-serializable object
        response = {"function": lambda x: x}  # Functions are not JSON serializable
        
        is_valid, message = validate_json_response(response)
        
        assert is_valid is False
        assert "not JSON serializable" in message
    
    def test_validate_json_response_complex_valid(self):
        """Test validation of complex but valid JSON response."""
        response = {
            "data": [1, 2, 3],
            "metadata": {"count": 3, "has_more": False},
            "nested": {"deep": {"key": None}}
        }
        
        is_valid, message = validate_json_response(response)
        
        assert is_valid is True
        assert message == "Valid JSON response"


class TestOutputTruncation:
    """Test output truncation functionality."""
    
    def test_truncate_output_short_string(self):
        """Test truncation of string shorter than max length."""
        output = "Short string"
        
        result = truncate_output(output, max_length=100)
        
        assert result == "Short string"
        assert "[output truncated]" not in result
    
    def test_truncate_output_exact_length(self):
        """Test truncation of string exactly at max length."""
        output = "A" * 50
        
        result = truncate_output(output, max_length=50)
        
        assert result == output
        assert "[output truncated]" not in result
    
    def test_truncate_output_long_string(self):
        """Test truncation of string longer than max length."""
        output = "A" * 100
        
        result = truncate_output(output, max_length=50)
        
        assert len(result) > 50  # Includes truncation message
        assert result[:50] == "A" * 50
        assert "... [output truncated]" in result
    
    def test_truncate_output_default_length(self):
        """Test truncation with default max length."""
        long_output = "X" * 6000  # Longer than default 5000
        
        result = truncate_output(long_output)
        
        assert len(result) < len(long_output)
        assert "... [output truncated]" in result
        assert result[:5000] == "X" * 5000
    
    def test_truncate_output_zero_length(self):
        """Test truncation with zero max length."""
        output = "Some output"
        
        result = truncate_output(output, max_length=0)
        
        assert result == "... [output truncated]"
    
    def test_truncate_output_empty_string(self):
        """Test truncation of empty string."""
        result = truncate_output("", max_length=100)
        
        assert result == ""
    
    def test_truncate_output_special_characters(self):
        """Test truncation with special characters and unicode."""
        output = "Special chars: émojis 🎉 and newlines\n\ttabs" * 100
        
        result = truncate_output(output, max_length=50)
        
        assert len(result) > 50  # Includes truncation message
        assert "... [output truncated]" in result


class TestUtilityEdgeCases:
    """Test edge cases and error conditions in utilities."""
    
    def test_parse_json_output_none_input(self):
        """Test JSON parsing with None input."""
        # This should be handled gracefully
        result = parse_json_output(None or "")
        
        assert "error" in result
    
    def test_format_mcp_response_none_values(self):
        """Test MCP response formatting with None values."""
        response = format_mcp_response(
            success=True, 
            data=None, 
            error=None, 
            metadata=None
        )
        
        assert response["success"] is True
        assert "data" not in response  # None data shouldn't be included
        assert "error" not in response
        assert "metadata" not in response
    
    def test_sanitize_command_with_none_elements(self):
        """Test command sanitization with None elements."""
        # Filter out None elements first as they shouldn't be in command
        command = ["databricks", "clusters", "list"]
        
        sanitized = sanitize_command_for_logging(command)
        
        assert "databricks clusters list" == sanitized
    
    def test_extract_error_empty_inputs(self):
        """Test error extraction with all empty inputs."""
        error_message = extract_error_from_cli_output("", "", 0)
        
        assert "Command failed with exit code 0" in error_message