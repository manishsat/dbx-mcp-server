"""
Base CLI executor for Databricks CLI operations.

This module provides the core functionality for executing Databricks CLI commands
with proper error handling, logging, and response parsing.
"""

import asyncio
import json
import logging
import subprocess
from typing import Any, Dict, List, Optional, Union

from src.core.config import get_databricks_cli_base_command, settings
from src.core.utils import (
    CLIError,
    extract_error_from_cli_output,
    parse_json_output,
    sanitize_command_for_logging,
    truncate_output
)

logger = logging.getLogger(__name__)


class DatabricksCLI:
    """Base class for executing Databricks CLI commands."""
    
    def __init__(self):
        """Initialize the CLI executor."""
        self.base_command = get_databricks_cli_base_command()
        self.timeout = settings.cli_timeout
        
    async def execute(
        self,
        command_args: List[str],
        input_data: Optional[str] = None,
        expect_json: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a Databricks CLI command.
        
        Args:
            command_args: List of command arguments (after 'databricks')
            input_data: Optional input data to pipe to the command
            expect_json: Whether to parse output as JSON
            
        Returns:
            Dictionary with command result
            
        Raises:
            CLIError: If command fails or returns invalid data
        """
        # Build full command
        full_command = self.base_command + command_args
        
        # Log the command (sanitized)
        safe_command = sanitize_command_for_logging(full_command)
        logger.debug(f"Executing command: {safe_command}")
        
        try:
            # Execute the command
            process = await asyncio.create_subprocess_exec(
                *full_command,
                stdin=subprocess.PIPE if input_data else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=None,
            )
            
            # Wait for completion with timeout
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                process.communicate(input=input_data.encode() if input_data else None),
                timeout=self.timeout
            )
            
            # Decode output
            stdout = stdout_bytes.decode('utf-8', errors='replace')
            stderr = stderr_bytes.decode('utf-8', errors='replace')
            exit_code = process.returncode
            
            # Log output for debugging
            if stdout:
                logger.debug(f"Command stdout: {truncate_output(stdout, 1000)}")
            if stderr:
                logger.debug(f"Command stderr: {truncate_output(stderr, 1000)}")
            
            # Handle command failure
            if exit_code != 0:
                error_message = extract_error_from_cli_output(stdout, stderr, exit_code)
                raise CLIError(
                    message=error_message,
                    command=full_command,
                    exit_code=exit_code,
                    stderr=stderr
                )
            
            # Parse output based on expectation
            if expect_json:
                result = parse_json_output(stdout)
                if "error" in result:
                    raise CLIError(
                        message=f"CLI returned error: {result['error']}",
                        command=full_command,
                        exit_code=0,
                        stderr=result.get("raw_output", "")
                    )
                return result
            else:
                return {"output": stdout.strip(), "success": True}
                
        except asyncio.TimeoutError:
            logger.error(f"Command timed out after {self.timeout} seconds: {safe_command}")
            raise CLIError(
                message=f"Command timed out after {self.timeout} seconds",
                command=full_command,
                exit_code=-1,
                stderr=""
            )
        except CLIError:
            # Re-raise CLI errors as-is
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing command: {str(e)}")
            raise CLIError(
                message=f"Unexpected error: {str(e)}",
                command=full_command,
                exit_code=-2,
                stderr=str(e)
            )
    
    async def execute_with_retry(
        self,
        command_args: List[str],
        input_data: Optional[str] = None,
        expect_json: bool = True,
        max_retries: int = 2,
        retry_delay: float = 1.0
    ) -> Dict[str, Any]:
        """
        Execute a command with retry logic for transient failures.
        
        Args:
            command_args: List of command arguments
            input_data: Optional input data
            expect_json: Whether to parse output as JSON
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            
        Returns:
            Dictionary with command result
        """
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                result = await self.execute(command_args, input_data, expect_json)
                if attempt > 0:
                    logger.info(f"Command succeeded on retry attempt {attempt}")
                return result
                
            except CLIError as e:
                last_exception = e
                
                # Don't retry on certain error types
                if e.exit_code in [1, 2] and "not found" in e.message.lower():
                    # Resource not found errors shouldn't be retried
                    raise
                
                if attempt < max_retries:
                    logger.warning(
                        f"Command failed (attempt {attempt + 1}/{max_retries + 1}): {e.message}. "
                        f"Retrying in {retry_delay} seconds..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Command failed after {max_retries + 1} attempts")
        
        # If we get here, all retries failed
        if last_exception:
            raise last_exception
    
    def validate_required_args(self, args: Dict[str, Any], required_fields: List[str]) -> None:
        """
        Validate that required arguments are present.
        
        Args:
            args: Arguments dictionary
            required_fields: List of required field names
            
        Raises:
            CLIError: If required fields are missing
        """
        missing_fields = []
        for field in required_fields:
            if field not in args or args[field] is None:
                missing_fields.append(field)
        
        if missing_fields:
            raise CLIError(
                message=f"Missing required arguments: {', '.join(missing_fields)}",
                command=[],
                exit_code=-1,
                stderr=""
            )
