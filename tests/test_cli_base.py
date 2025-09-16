"""
Tests for the CLI base class functionality.

This module tests the DatabricksCLI base class including command execution,
retry logic, error handling, timeout scenarios, and parameter validation.
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, Mock, patch
import subprocess
from src.cli.base import DatabricksCLI
from src.core.utils import CLIError


class TestDatabricksCLIBase:
    """Test the DatabricksCLI base class functionality."""
    
    def test_cli_initialization(self):
        """Test CLI instance initialization."""
        cli = DatabricksCLI()
        
        assert cli.base_command is not None
        assert isinstance(cli.base_command, list)
        assert cli.timeout > 0
        assert "databricks" in cli.base_command[0]
    
    def test_cli_initialization_with_profile(self):
        """Test CLI initialization with Databricks profile."""
        with patch('src.cli.base.get_databricks_cli_base_command') as mock_cmd:
            mock_cmd.return_value = ["databricks", "--profile", "test-profile"]
            
            cli = DatabricksCLI()
            assert cli.base_command == ["databricks", "--profile", "test-profile"]
    
    @pytest.mark.asyncio
    async def test_execute_success_json_output(self):
        """Test successful command execution with JSON output."""
        cli = DatabricksCLI()
        mock_output = '{"result": "success", "data": [1, 2, 3]}'
        
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.communicate = AsyncMock(return_value=(
            mock_output.encode('utf-8'), 
            b''
        ))
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_exec.return_value = mock_process
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (mock_output.encode('utf-8'), b'')
                
                result = await cli.execute(["clusters", "list"])
                
                assert result == {"result": "success", "data": [1, 2, 3]}
                mock_exec.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_success_non_json_output(self):
        """Test successful command execution with non-JSON output."""
        cli = DatabricksCLI()
        mock_output = "Command completed successfully"
        
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.communicate = AsyncMock(return_value=(
            mock_output.encode('utf-8'), 
            b''
        ))
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_exec.return_value = mock_process
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (mock_output.encode('utf-8'), b'')
                
                result = await cli.execute(["clusters", "list"], expect_json=False)
                
                assert result == {"output": "Command completed successfully", "success": True}
    
    @pytest.mark.asyncio
    async def test_execute_command_failure(self):
        """Test command execution with non-zero exit code."""
        cli = DatabricksCLI()
        stderr_output = "Error: Cluster not found"
        
        mock_process = Mock()
        mock_process.returncode = 1
        mock_process.communicate = AsyncMock(return_value=(
            b'', 
            stderr_output.encode('utf-8')
        ))
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_exec.return_value = mock_process
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (b'', stderr_output.encode('utf-8'))
                
                with pytest.raises(CLIError) as exc_info:
                    await cli.execute(["clusters", "get", "invalid-id"])
                
                assert exc_info.value.exit_code == 1
                assert "Cluster not found" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_execute_timeout(self):
        """Test command execution timeout."""
        cli = DatabricksCLI()
        cli.timeout = 0.1  # Very short timeout for testing
        
        mock_process = Mock()
        mock_process.communicate = AsyncMock()
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_exec.return_value = mock_process
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.side_effect = asyncio.TimeoutError()
                
                with pytest.raises(CLIError) as exc_info:
                    await cli.execute(["clusters", "list"])
                
                assert exc_info.value.exit_code == -1
                assert "timed out" in str(exc_info.value).lower()
    
    @pytest.mark.asyncio
    async def test_execute_invalid_json_output(self):
        """Test command execution with invalid JSON output."""
        cli = DatabricksCLI()
        invalid_json = "This is not valid JSON {"
        
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.communicate = AsyncMock(return_value=(
            invalid_json.encode('utf-8'), 
            b''
        ))
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_exec.return_value = mock_process
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (invalid_json.encode('utf-8'), b'')
                
                with pytest.raises(CLIError) as exc_info:
                    await cli.execute(["clusters", "list"])
                
                assert "Failed to parse JSON output" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_execute_with_input_data(self):
        """Test command execution with input data."""
        cli = DatabricksCLI()
        input_data = '{"cluster_name": "test"}'
        mock_output = '{"cluster_id": "123"}'
        
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.communicate = AsyncMock(return_value=(
            mock_output.encode('utf-8'), 
            b''
        ))
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_exec.return_value = mock_process
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (mock_output.encode('utf-8'), b'')
                
                result = await cli.execute(["clusters", "create"], input_data=input_data)
                
                assert result == {"cluster_id": "123"}
                
                # Verify subprocess was created with stdin pipe
                call_kwargs = mock_exec.call_args[1]
                assert call_kwargs['stdin'] == subprocess.PIPE
    
    @pytest.mark.asyncio
    async def test_execute_unexpected_exception(self):
        """Test command execution with unexpected exception."""
        cli = DatabricksCLI()
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_exec.side_effect = OSError("Command not found")
            
            with pytest.raises(CLIError) as exc_info:
                await cli.execute(["clusters", "list"])
            
            assert exc_info.value.exit_code == -2
            assert "Unexpected error" in str(exc_info.value)
            assert "Command not found" in str(exc_info.value)


class TestDatabricksCLIRetry:
    """Test retry functionality in DatabricksCLI."""
    
    @pytest.mark.asyncio
    async def test_execute_with_retry_success_first_attempt(self):
        """Test successful execution on first attempt with retry enabled."""
        cli = DatabricksCLI()
        expected_result = {"status": "success"}
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = expected_result
            
            result = await cli.execute_with_retry(["clusters", "list"], max_retries=2)
            
            assert result == expected_result
            assert mock_execute.call_count == 1
    
    @pytest.mark.asyncio
    async def test_execute_with_retry_success_after_failure(self):
        """Test successful execution after initial failures."""
        cli = DatabricksCLI()
        expected_result = {"status": "success"}
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            # First call fails, second succeeds
            mock_execute.side_effect = [
                CLIError("Temporary failure", [], 500, "Server error"),
                expected_result
            ]
            
            result = await cli.execute_with_retry(["clusters", "list"], max_retries=2, retry_delay=0.01)
            
            assert result == expected_result
            assert mock_execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_execute_with_retry_max_retries_exceeded(self):
        """Test retry logic when max retries is exceeded."""
        cli = DatabricksCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.side_effect = CLIError("Persistent failure", [], 500, "Server error")
            
            with pytest.raises(CLIError) as exc_info:
                await cli.execute_with_retry(["clusters", "list"], max_retries=2, retry_delay=0.01)
            
            assert str(exc_info.value) == "CLI Error: Persistent failure (exit code 500)"
            assert mock_execute.call_count == 3  # Initial + 2 retries
    
    @pytest.mark.asyncio
    async def test_execute_with_retry_no_retry_on_not_found(self):
        """Test that certain errors are not retried (e.g., resource not found)."""
        cli = DatabricksCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.side_effect = CLIError("Resource not found", [], 1, "Not found")
            
            with pytest.raises(CLIError):
                await cli.execute_with_retry(["clusters", "get", "invalid-id"], max_retries=2)
            
            # Should not retry for "not found" errors
            assert mock_execute.call_count == 1
    
    @pytest.mark.asyncio
    async def test_execute_with_retry_delay_behavior(self):
        """Test that retry delay is respected."""
        cli = DatabricksCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.side_effect = CLIError("Temporary failure", [], 500, "Server error")
            
            with patch('asyncio.sleep') as mock_sleep:
                try:
                    await cli.execute_with_retry(["clusters", "list"], max_retries=1, retry_delay=1.5)
                except CLIError:
                    pass  # Expected to fail
                
                mock_sleep.assert_called_with(1.5)


class TestDatabricksCLIValidation:
    """Test parameter validation in DatabricksCLI."""
    
    def test_validate_required_args_success(self):
        """Test successful validation of required arguments."""
        cli = DatabricksCLI()
        args = {"cluster_id": "123", "cluster_name": "test"}
        required = ["cluster_id", "cluster_name"]
        
        # Should not raise any exception
        cli.validate_required_args(args, required)
    
    def test_validate_required_args_missing_single(self):
        """Test validation failure for single missing argument."""
        cli = DatabricksCLI()
        args = {"cluster_name": "test"}
        required = ["cluster_id", "cluster_name"]
        
        with pytest.raises(CLIError) as exc_info:
            cli.validate_required_args(args, required)
        
        assert "Missing required arguments: cluster_id" in str(exc_info.value)
        assert exc_info.value.exit_code == -1
    
    def test_validate_required_args_missing_multiple(self):
        """Test validation failure for multiple missing arguments."""
        cli = DatabricksCLI()
        args = {}
        required = ["cluster_id", "cluster_name", "node_type"]
        
        with pytest.raises(CLIError) as exc_info:
            cli.validate_required_args(args, required)
        
        error_message = str(exc_info.value)
        assert "Missing required arguments:" in error_message
        assert "cluster_id" in error_message
        assert "cluster_name" in error_message
        assert "node_type" in error_message
    
    def test_validate_required_args_none_values(self):
        """Test validation failure for None values."""
        cli = DatabricksCLI()
        args = {"cluster_id": None, "cluster_name": "test"}
        required = ["cluster_id", "cluster_name"]
        
        with pytest.raises(CLIError) as exc_info:
            cli.validate_required_args(args, required)
        
        assert "Missing required arguments: cluster_id" in str(exc_info.value)
    
    def test_validate_required_args_empty_required_list(self):
        """Test validation with empty required list."""
        cli = DatabricksCLI()
        args = {"optional_param": "value"}
        required = []
        
        # Should not raise any exception
        cli.validate_required_args(args, required)


class TestDatabricksCLICommandBuilding:
    """Test command building and execution setup."""
    
    @pytest.mark.asyncio
    async def test_command_building_basic(self):
        """Test basic command building."""
        cli = DatabricksCLI()
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_process = Mock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b'{}', b''))
            mock_exec.return_value = mock_process
            
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (b'{}', b'')
                
                await cli.execute(["clusters", "list", "--output", "json"])
                
                # Verify command was built correctly
                call_args = mock_exec.call_args[0]
                expected_command = cli.base_command + ["clusters", "list", "--output", "json"]
                assert list(call_args) == expected_command
    
    @pytest.mark.asyncio
    async def test_subprocess_configuration(self):
        """Test subprocess configuration parameters."""
        cli = DatabricksCLI()
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_process = Mock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b'{}', b''))
            mock_exec.return_value = mock_process
            
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (b'{}', b'')
                
                await cli.execute(["clusters", "list"])
                
                # Verify subprocess configuration
                call_kwargs = mock_exec.call_args[1]
                assert call_kwargs['stdout'] == subprocess.PIPE
                assert call_kwargs['stderr'] == subprocess.PIPE
                assert call_kwargs['cwd'] is None
                assert call_kwargs['stdin'] is None  # No input data
    
    @pytest.mark.asyncio
    async def test_subprocess_with_input_configuration(self):
        """Test subprocess configuration with input data."""
        cli = DatabricksCLI()
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_process = Mock()
            mock_process.returncode = 0
            mock_process.communicate = AsyncMock(return_value=(b'{}', b''))
            mock_exec.return_value = mock_process
            
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (b'{}', b'')
                
                await cli.execute(["clusters", "create"], input_data='{"test": "data"}')
                
                # Verify subprocess configuration
                call_kwargs = mock_exec.call_args[1]
                assert call_kwargs['stdin'] == subprocess.PIPE