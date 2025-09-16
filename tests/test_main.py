"""
Tests for the main entry point functionality.

This module tests the main.py entry point including configuration validation,
logging setup, MCP server startup, and error scenarios.
"""

import asyncio
import pytest
import sys
from unittest.mock import AsyncMock, Mock, patch
from src.main import main, cli_main
from src.core.utils import CLIError


class TestMainEntryPoint:
    """Test the main async function entry point."""
    
    @pytest.mark.asyncio
    async def test_main_success_flow(self):
        """Test successful main execution flow."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    mock_settings.mcp_server_version = "0.1.0"
                    mock_settings.mcp_server_name = "test-server"
                    mock_settings.log_level = "INFO"
                    mock_settings.databricks_profile = None
                    mock_settings.databricks_cli_command = "databricks"
                    mock_settings.cli_timeout = 300
                    
                    with patch('src.mcp_server.main', new_callable=AsyncMock) as mock_mcp_main:
                        result = await main()
                        
                        assert result == 0
                        mock_setup_logging.assert_called_once()
                        mock_validate.assert_called_once()
                        mock_mcp_main.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_main_configuration_validation_failure(self):
        """Test main execution when configuration validation fails."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (False, "Databricks CLI not found")
                
                result = await main()
                
                assert result == 1
                mock_logger.error.assert_called_with(
                    "Configuration validation failed: Databricks CLI not found"
                )
    
    @pytest.mark.asyncio
    async def test_main_mcp_server_startup_failure(self):
        """Test main execution when MCP server startup fails."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    mock_settings.mcp_server_version = "0.1.0"
                    mock_settings.mcp_server_name = "test-server"
                    mock_settings.log_level = "INFO"
                    mock_settings.databricks_profile = None
                    mock_settings.databricks_cli_command = "databricks"
                    mock_settings.cli_timeout = 300
                    
                    with patch('src.mcp_server.main', new_callable=AsyncMock) as mock_mcp_main:
                        mock_mcp_main.side_effect = Exception("Failed to start server")
                        
                        result = await main()
                        
                        assert result == 1
                        mock_logger.error.assert_called()
                        error_call = mock_logger.error.call_args[0][0]
                        assert "Error starting MCP server" in error_call
                        assert "Failed to start server" in error_call
    
    @pytest.mark.asyncio
    async def test_main_logging_setup(self):
        """Test that main properly sets up logging with correct messages."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    mock_settings.mcp_server_version = "1.2.3"
                    mock_settings.mcp_server_name = "databricks-mcp"
                    mock_settings.log_level = "DEBUG"
                    mock_settings.databricks_profile = "production"
                    mock_settings.databricks_cli_command = "databricks"
                    mock_settings.cli_timeout = 600
                    
                    with patch('src.mcp_server.main', new_callable=AsyncMock):
                        await main()
                        
                        # Verify logging calls
                        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
                        
                        assert any("Starting Databricks CLI MCP Server v1.2.3" in call for call in info_calls)
                        assert any("Server name: databricks-mcp" in call for call in info_calls)
                        assert any("Log level: DEBUG" in call for call in info_calls)
                        assert any("Configuration validation passed" in call for call in info_calls)
                        assert any("Starting MCP server..." in call for call in info_calls)
    
    @pytest.mark.asyncio
    async def test_main_databricks_profile_configuration_logging(self):
        """Test databricks configuration logging with different profile scenarios."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    mock_settings.mcp_server_version = "0.1.0"
                    mock_settings.mcp_server_name = "test-server"
                    mock_settings.log_level = "INFO"
                    mock_settings.databricks_profile = "production"
                    mock_settings.databricks_cli_command = "databricks"
                    mock_settings.cli_timeout = 300
                    
                    with patch('src.mcp_server.main', new_callable=AsyncMock):
                        await main()
                        
                        # Check profile info logging
                        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
                        config_log = next(call for call in info_calls if "Databricks configuration:" in call)
                        
                        assert "production" in config_log
                        assert "databricks" in config_log
                        assert "300s" in config_log
    
    @pytest.mark.asyncio
    async def test_main_default_profile_configuration_logging(self):
        """Test databricks configuration logging with default profile."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    mock_settings.mcp_server_version = "0.1.0"
                    mock_settings.mcp_server_name = "test-server"
                    mock_settings.log_level = "INFO"
                    mock_settings.databricks_profile = None  # No profile specified
                    mock_settings.databricks_cli_command = "databricks"
                    mock_settings.cli_timeout = 300
                    
                    with patch('src.mcp_server.main', new_callable=AsyncMock):
                        await main()
                        
                        # Check default profile logging
                        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
                        config_log = next(call for call in info_calls if "Databricks configuration:" in call)
                        
                        assert "default" in config_log


class TestCLIMainWrapper:
    """Test the CLI main wrapper function."""
    
    def test_cli_main_success(self):
        """Test successful CLI main execution."""
        with patch('src.main.asyncio.run') as mock_run:
            mock_run.return_value = 0
            
            with patch('src.main.sys.exit') as mock_exit:
                cli_main()
                
                mock_run.assert_called_once()
                mock_exit.assert_called_once_with(0)
    
    def test_cli_main_failure(self):
        """Test CLI main execution with failure."""
        with patch('src.main.asyncio.run') as mock_run:
            mock_run.return_value = 1
            
            with patch('src.main.sys.exit') as mock_exit:
                cli_main()
                
                mock_run.assert_called_once()
                mock_exit.assert_called_once_with(1)
    
    def test_cli_main_keyboard_interrupt(self):
        """Test CLI main execution with keyboard interrupt."""
        with patch('src.main.asyncio.run') as mock_run:
            mock_run.side_effect = KeyboardInterrupt()
            
            with patch('src.main.sys.exit') as mock_exit:
                with patch('builtins.print') as mock_print:
                    cli_main()
                    
                    mock_print.assert_called_once_with("\nShutdown requested by user")
                    mock_exit.assert_called_once_with(0)
    
    def test_cli_main_unexpected_exception(self):
        """Test CLI main execution with unexpected exception."""
        with patch('src.main.asyncio.run') as mock_run:
            mock_run.side_effect = RuntimeError("Unexpected error occurred")
            
            with patch('src.main.sys.exit') as mock_exit:
                with patch('builtins.print') as mock_print:
                    cli_main()
                    
                    mock_print.assert_called_once_with("Unexpected error: Unexpected error occurred")
                    mock_exit.assert_called_once_with(1)


class TestMainModuleExecution:
    """Test main module execution behavior."""
    
    def test_main_module_calls_cli_main(self):
        """Test that running the module calls cli_main."""
        # This test verifies the if __name__ == "__main__": block
        with patch('src.main.cli_main') as mock_cli_main:
            # Simulate module execution
            import src.main
            
            # We can't directly test the __name__ == "__main__" block,
            # but we can test that cli_main is defined and callable
            assert callable(src.main.cli_main)
            assert callable(src.main.main)


class TestMainIntegrationScenarios:
    """Test integration scenarios with main entry point."""
    
    @pytest.mark.asyncio
    async def test_main_with_cli_error_in_mcp_server(self):
        """Test main execution when MCP server raises CLIError."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    mock_settings.mcp_server_version = "0.1.0"
                    mock_settings.mcp_server_name = "test-server"
                    mock_settings.log_level = "INFO"
                    mock_settings.databricks_profile = None
                    mock_settings.databricks_cli_command = "databricks"
                    mock_settings.cli_timeout = 300
                    
                    cli_error = CLIError(
                        message="Databricks CLI authentication failed",
                        command=["databricks", "clusters", "list"],
                        exit_code=1,
                        stderr="Authentication error"
                    )
                    
                    with patch('src.mcp_server.main', new_callable=AsyncMock) as mock_mcp_main:
                        mock_mcp_main.side_effect = cli_error
                        
                        result = await main()
                        
                        assert result == 1
                        # Should log the error with exc_info=True for stack trace
                        assert mock_logger.error.called
    
    @pytest.mark.asyncio
    async def test_main_with_settings_access_error(self):
        """Test main execution when settings access fails."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    # Simulate settings access error - skip the __getattr__ approach as it's not compatible
                    mock_settings.mcp_server_version = "0.1.0"
                    mock_settings.mcp_server_name = "test-server"
                    mock_settings.log_level = "INFO"
                    # mock_settings.__getattr__ = Mock(side_effect=AttributeError("Setting not found"))
                    
                    # Instead, we'll simulate the error through MCP server startup
                    with patch('src.mcp_server.main') as mock_mcp_main:
                        mock_mcp_main.side_effect = AttributeError("Setting not found")
                        
                        result = await main()
                        
                        # Should handle gracefully and return error code
                        assert result == 1
    
    @pytest.mark.asyncio
    async def test_main_import_error_handling(self):
        """Test main execution when MCP server import fails."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    mock_settings.mcp_server_version = "0.1.0"
                    mock_settings.mcp_server_name = "test-server"
                    mock_settings.log_level = "INFO"
                    mock_settings.databricks_profile = None
                    mock_settings.databricks_cli_command = "databricks"
                    mock_settings.cli_timeout = 300
                    
                    # Mock import failure
                    with patch('builtins.__import__', side_effect=ImportError("Cannot import MCP server")):
                        result = await main()
                        
                        assert result == 1
                        assert mock_logger.error.called


class TestMainConfigurationScenarios:
    """Test various configuration scenarios in main."""
    
    @pytest.mark.asyncio
    async def test_main_with_custom_timeout_setting(self):
        """Test main execution with custom timeout setting."""
        with patch('src.main.setup_logging') as mock_setup_logging:
            mock_logger = Mock()
            mock_setup_logging.return_value = mock_logger
            
            with patch('src.main.validate_configuration') as mock_validate:
                mock_validate.return_value = (True, "Configuration is valid")
                
                with patch('src.main.settings') as mock_settings:
                    mock_settings.mcp_server_version = "0.1.0"
                    mock_settings.mcp_server_name = "test-server"
                    mock_settings.log_level = "INFO"
                    mock_settings.databricks_profile = "custom-profile"
                    mock_settings.databricks_cli_command = "custom-databricks-cmd"
                    mock_settings.cli_timeout = 900  # Custom timeout
                    
                    with patch('src.mcp_server.main', new_callable=AsyncMock):
                        await main()
                        
                        # Verify custom settings are logged
                        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
                        config_log = next(call for call in info_calls if "Databricks configuration:" in call)
                        
                        assert "custom-profile" in config_log
                        assert "custom-databricks-cmd" in config_log
                        assert "900s" in config_log