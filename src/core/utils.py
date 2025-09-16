"""
Utility functions for the Databricks CLI MCP server.

This module provides common utilities for logging, error handling,
and data processing.
"""

import json
import logging
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

from src.core.config import settings


def setup_logging() -> logging.Logger:
    """
    Set up logging configuration for the MCP server.
    
    Returns:
        Configured logger instance
    """
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stderr),  # Use stderr to avoid MCP protocol conflicts
        ],
    )
    
    # Create and return logger for this module
    logger = logging.getLogger("dbx_mcp_server")
    logger.setLevel(getattr(logging, settings.log_level))
    
    return logger


class CLIError(Exception):
    """Exception raised for errors in Databricks CLI operations."""
    
    def __init__(self, message: str, command: List[str], exit_code: int, stderr: str = ""):
        self.message = message
        self.command = command
        self.exit_code = exit_code
        self.stderr = stderr
        super().__init__(self.message)
    
    def __str__(self) -> str:
        return f"CLI Error: {self.message} (exit code {self.exit_code})"


def format_mcp_response(
    success: bool, 
    data: Optional[Union[Dict[str, Any], List[Any]]] = None, 
    error: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Format a response for MCP client consumption.
    
    Args:
        success: Whether the operation was successful
        data: The response data
        error: Error message if operation failed
        metadata: Additional metadata
    
    Returns:
        Formatted response dictionary
    """
    response = {
        "success": success,
        "timestamp": None,  # Will be added by server if needed
    }
    
    if success and data is not None:
        response["data"] = data
    
    if not success and error:
        response["error"] = error
    
    if metadata:
        response["metadata"] = metadata
    
    return response


def parse_json_output(output: str, fallback_message: str = "No data returned") -> Dict[str, Any]:
    """
    Parse JSON output from CLI commands with error handling.
    
    Args:
        output: Raw output string
        fallback_message: Message to use if parsing fails
    
    Returns:
        Parsed JSON data or error response
    """
    if not output.strip():
        return {"error": fallback_message}
    
    try:
        return json.loads(output)
    except json.JSONDecodeError as e:
        return {
            "error": f"Failed to parse JSON output: {str(e)}",
            "raw_output": output[:1000]  # Truncate long output
        }


def sanitize_command_for_logging(command: List[str]) -> str:
    """
    Sanitize command for logging by masking sensitive information.
    
    Args:
        command: Command parts list
    
    Returns:
        Sanitized command string for logging
    """
    # Create a copy to avoid modifying the original
    safe_command = command.copy()
    
    # Mask tokens and sensitive values
    for i, part in enumerate(safe_command):
        if "token" in part.lower() or "password" in part.lower():
            if i + 1 < len(safe_command):
                safe_command[i + 1] = "***MASKED***"
        elif any(sensitive in part.lower() for sensitive in ["dapi", "pat-"]):
            safe_command[i] = "***MASKED***"
        elif "bearer" in part.lower():
            # Handle "Bearer token" format - could be separate args or combined
            safe_command[i] = "***MASKED***"
    
    return " ".join(safe_command)


def validate_json_response(response: Any) -> Tuple[bool, str]:
    """
    Validate that a response is properly formatted JSON.
    
    Args:
        response: Response to validate
    
    Returns:
        Tuple of (is_valid, message)
    """
    if not isinstance(response, (dict, list)):
        return False, "Response is not a valid JSON object or array"
    
    try:
        # Try to serialize it to ensure it's JSON-serializable
        json.dumps(response)
        return True, "Valid JSON response"
    except (TypeError, ValueError) as e:
        return False, f"Response is not JSON serializable: {str(e)}"


def truncate_output(output: str, max_length: int = 5000) -> str:
    """
    Truncate output for logging or display purposes.
    
    Args:
        output: Output string to truncate
        max_length: Maximum length to keep
    
    Returns:
        Truncated output with ellipsis if truncated
    """
    if len(output) <= max_length:
        return output
    
    return output[:max_length] + "... [output truncated]"


def extract_error_from_cli_output(stdout: str, stderr: str, exit_code: int) -> str:
    """
    Extract meaningful error message from CLI output.
    
    Args:
        stdout: Standard output
        stderr: Standard error
        exit_code: Command exit code
    
    Returns:
        Formatted error message
    """
    error_parts = []
    
    if stderr:
        error_parts.append(f"Error: {stderr.strip()}")
    
    if stdout and "error" in stdout.lower():
        try:
            # Try to parse as JSON to extract error
            data = json.loads(stdout)
            if isinstance(data, dict):
                # Look for common error message fields
                error_msg = (data.get("error") or 
                           data.get("message") or 
                           data.get("error_message") or
                           data.get("detail"))
                if error_msg:
                    error_parts.append(error_msg)
        except json.JSONDecodeError:
            # If not JSON, include relevant stdout
            error_parts.append(f"Output: {truncate_output(stdout, 500)}")
    
    if not error_parts:
        error_parts.append(f"Command failed with exit code {exit_code}")
    
    return " | ".join(error_parts)
