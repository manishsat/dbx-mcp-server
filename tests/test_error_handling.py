"""
Tests for error handling functionality.

This module tests CLIError exceptions, error parsing, timeout handling,
and failure scenarios across the system.
"""

import asyncio
import json
import pytest
from unittest.mock import Mock, patch
from src.core.utils import CLIError, extract_error_from_cli_output, parse_json_output
from src.cli.base import DatabricksCLI


class TestCLIError:
    """Test CLIError exception functionality."""
    
    def test_cli_error_creation_basic(self):
        """Test basic CLIError creation."""
        error = CLIError(
            message="Test error message",
            command=["databricks", "clusters", "list"],
            exit_code=1,
            stderr="Error details"
        )
        
        assert str(error) == "CLI Error: Test error message (exit code 1)"
        assert error.message == "Test error message"
        assert error.command == ["databricks", "clusters", "list"]
        assert error.exit_code == 1
        assert error.stderr == "Error details"
    
    def test_cli_error_creation_minimal(self):
        """Test CLIError creation with minimal parameters."""
        error = CLIError(
            message="Simple error",
            command=[],
            exit_code=2
        )
        
        assert str(error) == "CLI Error: Simple error (exit code 2)"
        assert error.stderr == ""  # Default empty stderr
    
    def test_cli_error_inheritance(self):
        """Test that CLIError properly inherits from Exception."""
        error = CLIError("Test", [], 1)
        
        assert isinstance(error, Exception)
        assert isinstance(error, CLIError)
    
    def test_cli_error_message_accessibility(self):
        """Test that CLIError message is accessible through str() and .message."""
        message = "Detailed error information"
        error = CLIError(message, [], 1)
        
        assert error.message == message
        assert message in str(error)


class TestErrorParsing:
    """Test error message parsing and extraction."""
    
    def test_extract_error_from_stderr_only(self):
        """Test error extraction when only stderr is present."""
        stdout = ""
        stderr = "Authentication failed: Invalid token"
        exit_code = 1
        
        error_message = extract_error_from_cli_output(stdout, stderr, exit_code)
        
        assert "Error: Authentication failed: Invalid token" in error_message
    
    def test_extract_error_from_json_stdout(self):
        """Test error extraction from JSON error in stdout."""
        stdout = '{"error": "Cluster not found", "error_code": "RESOURCE_NOT_FOUND"}'
        stderr = ""
        exit_code = 1
        
        error_message = extract_error_from_cli_output(stdout, stderr, exit_code)
        
        assert "Cluster not found" in error_message
    
    def test_extract_error_from_stdout_and_stderr(self):
        """Test error extraction when both stdout and stderr have errors."""
        stdout = '{"error": "Invalid request format"}'
        stderr = "HTTP 400: Bad Request"
        exit_code = 1
        
        error_message = extract_error_from_cli_output(stdout, stderr, exit_code)
        
        assert "Error: HTTP 400: Bad Request" in error_message
        assert "Invalid request format" in error_message
    
    def test_extract_error_from_non_json_stdout(self):
        """Test error extraction from non-JSON stdout containing 'error'."""
        stdout = "Error: Failed to connect to Databricks workspace"
        stderr = ""
        exit_code = 1
        
        error_message = extract_error_from_cli_output(stdout, stderr, exit_code)
        
        assert "Output: Error: Failed to connect to Databricks workspace" in error_message
    
    def test_extract_error_fallback_exit_code(self):
        """Test error extraction fallback to exit code when no other info."""
        stdout = "Some normal output"
        stderr = ""
        exit_code = 2
        
        error_message = extract_error_from_cli_output(stdout, stderr, exit_code)
        
        assert "Command failed with exit code 2" in error_message
    
    def test_extract_error_long_output_truncation(self):
        """Test that long error output gets truncated."""
        stdout = "Normal output with no error indicators"
        stderr = "A" * 1000  # Long error message
        exit_code = 1
        
        error_message = extract_error_from_cli_output(stdout, stderr, exit_code)
        
        # Should contain error but be reasonably sized
        assert "Error:" in error_message
        assert len(error_message) < 2000  # Should be truncated
    
    def test_extract_error_invalid_json_in_stdout(self):
        """Test error extraction when stdout contains invalid JSON with 'error'."""
        stdout = '{"error": "Missing quote}'  # Invalid JSON
        stderr = ""
        exit_code = 1
        
        error_message = extract_error_from_cli_output(stdout, stderr, exit_code)
        
        # Should fall back to treating it as regular output
        assert "Output:" in error_message
        assert '{"error": "Missing quote}' in error_message[:100]  # Check truncated portion


class TestJSONOutputParsing:
    """Test JSON output parsing error handling."""
    
    def test_parse_json_output_valid_json(self):
        """Test parsing valid JSON output."""
        json_output = '{"clusters": [{"cluster_id": "123", "state": "RUNNING"}]}'
        
        result = parse_json_output(json_output)
        
        expected = {"clusters": [{"cluster_id": "123", "state": "RUNNING"}]}
        assert result == expected
    
    def test_parse_json_output_empty_string(self):
        """Test parsing empty output."""
        result = parse_json_output("")
        
        assert result == {"error": "No data returned"}
    
    def test_parse_json_output_whitespace_only(self):
        """Test parsing whitespace-only output."""
        result = parse_json_output("   \n  \t  ")
        
        assert result == {"error": "No data returned"}
    
    def test_parse_json_output_invalid_json(self):
        """Test parsing invalid JSON output."""
        invalid_json = '{"incomplete": json'
        
        result = parse_json_output(invalid_json)
        
        assert "error" in result
        assert "Failed to parse JSON output" in result["error"]
        assert "raw_output" in result
        assert result["raw_output"] == invalid_json
    
    def test_parse_json_output_custom_fallback_message(self):
        """Test parsing with custom fallback message."""
        result = parse_json_output("", "Custom no data message")
        
        assert result == {"error": "Custom no data message"}
    
    def test_parse_json_output_long_invalid_json(self):
        """Test parsing very long invalid JSON (should be truncated)."""
        long_invalid = "Invalid JSON: " + "A" * 2000
        
        result = parse_json_output(long_invalid)
        
        assert "error" in result
        assert "raw_output" in result
        assert len(result["raw_output"]) <= 1000  # Should be truncated


class TestTimeoutHandling:
    """Test timeout handling scenarios."""
    
    @pytest.mark.asyncio
    async def test_cli_timeout_scenario(self):
        """Test CLI timeout behavior."""
        cli = DatabricksCLI()
        cli.timeout = 0.001  # Very short timeout
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_process = Mock()
            mock_process.communicate = Mock()  # Will timeout
            mock_exec.return_value = mock_process
            
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.side_effect = asyncio.TimeoutError("Timeout occurred")
                
                with pytest.raises(CLIError) as exc_info:
                    await cli.execute(["clusters", "list"])
                
                error = exc_info.value
                assert error.exit_code == -1
                assert "timed out" in error.message.lower()
                assert error.command[-2:] == ["clusters", "list"]
    
    @pytest.mark.asyncio
    async def test_timeout_with_retry_logic(self):
        """Test timeout behavior with retry logic."""
        cli = DatabricksCLI()
        
        timeout_error = CLIError("Command timed out", [], -1, "")
        
        with patch.object(cli, 'execute', side_effect=timeout_error):
            with pytest.raises(CLIError) as exc_info:
                await cli.execute_with_retry(
                    ["clusters", "list"], 
                    max_retries=2, 
                    retry_delay=0.001
                )
            
            # Timeout errors should be retried
            assert exc_info.value.message == "Command timed out"


class TestNetworkAndConnectivityErrors:
    """Test network and connectivity error scenarios."""
    
    def test_connection_refused_error_parsing(self):
        """Test parsing of connection refused errors."""
        stderr = "curl: (7) Failed to connect to workspace.databricks.com port 443: Connection refused"
        
        error_message = extract_error_from_cli_output("", stderr, 7)
        
        assert "Connection refused" in error_message
        assert "Failed to connect" in error_message
    
    def test_authentication_error_parsing(self):
        """Test parsing of authentication errors."""
        stdout = '{"error_code": "INVALID_REQUEST", "message": "Invalid authentication token"}'
        
        error_message = extract_error_from_cli_output(stdout, "", 1)
        
        assert "Invalid authentication token" in error_message
    
    def test_permission_denied_error_parsing(self):
        """Test parsing of permission denied errors."""
        stdout = '{"error": "User does not have permission to access this resource"}'
        
        error_message = extract_error_from_cli_output(stdout, "", 1)
        
        assert "does not have permission" in error_message
    
    def test_rate_limit_error_parsing(self):
        """Test parsing of rate limiting errors."""
        stderr = "HTTP 429: Too Many Requests"
        
        error_message = extract_error_from_cli_output("", stderr, 1)
        
        assert "Too Many Requests" in error_message
        assert "HTTP 429" in error_message


class TestCLIErrorInDifferentScenarios:
    """Test CLIError behavior in various CLI operation scenarios."""
    
    @pytest.mark.asyncio
    async def test_cluster_not_found_error(self):
        """Test error handling for cluster not found scenario."""
        cli = DatabricksCLI()
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_process = Mock()
            mock_process.returncode = 1
            mock_process.communicate = Mock(return_value=(
                b'{"error": "Cluster not found"}', 
                b'HTTP 404: Not Found'
            ))
            mock_exec.return_value = mock_process
            
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (
                    b'{"error": "Cluster not found"}', 
                    b'HTTP 404: Not Found'
                )
                
                with pytest.raises(CLIError) as exc_info:
                    await cli.execute(["clusters", "get", "invalid-cluster-id"])
                
                error = exc_info.value
                assert error.exit_code == 1
                assert "Cluster not found" in error.message or "HTTP 404" in error.message
    
    @pytest.mark.asyncio
    async def test_invalid_parameter_error(self):
        """Test error handling for invalid parameters."""
        cli = DatabricksCLI()
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_process = Mock()
            mock_process.returncode = 2
            mock_process.communicate = Mock(return_value=(
                b'', 
                b'Error: Invalid parameter --invalid-flag'
            ))
            mock_exec.return_value = mock_process
            
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (
                    b'', 
                    b'Error: Invalid parameter --invalid-flag'
                )
                
                with pytest.raises(CLIError) as exc_info:
                    await cli.execute(["clusters", "list", "--invalid-flag"])
                
                error = exc_info.value
                assert error.exit_code == 2
                assert "Invalid parameter" in error.message
    
    @pytest.mark.asyncio
    async def test_workspace_permission_error(self):
        """Test error handling for workspace permission errors."""
        cli = DatabricksCLI()
        
        error_response = {
            "error_code": "PERMISSION_DENIED",
            "message": "User does not have access to workspace"
        }
        
        with patch('asyncio.create_subprocess_exec') as mock_exec:
            mock_process = Mock()
            mock_process.returncode = 1
            mock_process.communicate = Mock(return_value=(
                json.dumps(error_response).encode('utf-8'), 
                b''
            ))
            mock_exec.return_value = mock_process
            
            with patch('asyncio.wait_for') as mock_wait:
                mock_wait.return_value = (
                    json.dumps(error_response).encode('utf-8'), 
                    b''
                )
                
                with pytest.raises(CLIError) as exc_info:
                    await cli.execute(["workspace", "list", "/"])
                
                error = exc_info.value
                assert "User does not have access" in error.message


class TestErrorRecoveryScenarios:
    """Test error recovery and retry scenarios."""
    
    @pytest.mark.asyncio
    async def test_transient_error_recovery(self):
        """Test recovery from transient errors."""
        cli = DatabricksCLI()
        
        # First call fails with transient error, second succeeds
        success_response = {"clusters": []}
        
        with patch.object(cli, 'execute') as mock_execute:
            mock_execute.side_effect = [
                CLIError("Temporary server error", [], 500, "Internal Server Error"),
                success_response
            ]
            
            result = await cli.execute_with_retry(
                ["clusters", "list"], 
                max_retries=1, 
                retry_delay=0.001
            )
            
            assert result == success_response
            assert mock_execute.call_count == 2
    
    @pytest.mark.asyncio
    async def test_permanent_error_no_recovery(self):
        """Test that permanent errors are not retried."""
        cli = DatabricksCLI()
        
        permanent_error = CLIError("Resource not found", [], 1, "404 Not Found")
        
        with patch.object(cli, 'execute') as mock_execute:
            mock_execute.side_effect = permanent_error
            
            with pytest.raises(CLIError):
                await cli.execute_with_retry(
                    ["clusters", "get", "invalid-id"], 
                    max_retries=2
                )
            
            # Should not retry for "not found" errors
            assert mock_execute.call_count == 1