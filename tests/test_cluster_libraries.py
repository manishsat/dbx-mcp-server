"""
Tests for cluster library management functionality.
"""

import json
import pytest
from unittest.mock import AsyncMock, Mock, patch

from src.cli.clusters import ClustersCLI
from src.core.utils import CLIError


class TestClusterLibraryManagement:
    """Test cluster library management operations."""
    
    @pytest.mark.asyncio
    async def test_install_libraries_maven_success(self):
        """Test successful Maven library installation."""
        cli = ClustersCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"success": True}
            
            libraries = [
                {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}}
            ]
            
            result = await cli.install_libraries("test-cluster-id", libraries)
            
            # Verify the result
            assert result["success"] is True
            assert result["cluster_id"] == "test-cluster-id"
            assert result["libraries_count"] == 1
            assert "asynchronous operation" in result["message"]
            assert result["libraries"] == libraries
            
            # Verify the CLI command was called correctly
            mock_execute.assert_called_once()
            args = mock_execute.call_args[0][0]
            assert args[:2] == ["libraries", "install"]
            assert "--json" in args
            
            # Verify the JSON payload
            json_arg_index = args.index("--json") + 1
            json_payload = json.loads(args[json_arg_index])
            assert json_payload["cluster_id"] == "test-cluster-id"
            assert json_payload["libraries"] == libraries
    
    @pytest.mark.asyncio
    async def test_install_libraries_pypi_success(self):
        """Test successful PyPI library installation."""
        cli = ClustersCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"success": True}
            
            libraries = [
                {"pypi": {"package": "pandas==1.3.0"}},
                {"pypi": {"package": "numpy"}}
            ]
            
            result = await cli.install_libraries("test-cluster-id", libraries)
            
            assert result["success"] is True
            assert result["libraries_count"] == 2
    
    @pytest.mark.asyncio
    async def test_install_libraries_wheel_success(self):
        """Test successful wheel library installation."""
        cli = ClustersCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"success": True}
            
            libraries = [
                {"wheel": "/Workspace/path/to/library.whl"}
            ]
            
            result = await cli.install_libraries("test-cluster-id", libraries)
            
            assert result["success"] is True
            assert result["libraries_count"] == 1
    
    @pytest.mark.asyncio
    async def test_install_libraries_mixed_types(self):
        """Test installation of mixed library types."""
        cli = ClustersCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"success": True}
            
            libraries = [
                {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}},
                {"pypi": {"package": "pandas==1.3.0"}},
                {"wheel": "/Workspace/path/to/library.whl"}
            ]
            
            result = await cli.install_libraries("test-cluster-id", libraries)
            
            assert result["success"] is True
            assert result["libraries_count"] == 3
    
    @pytest.mark.asyncio
    async def test_install_libraries_missing_cluster_id(self):
        """Test library installation with missing cluster_id."""
        cli = ClustersCLI()
        
        libraries = [{"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}}]
        
        with pytest.raises(ValueError, match="cluster_id is required"):
            await cli.install_libraries("", libraries)
    
    @pytest.mark.asyncio
    async def test_install_libraries_empty_libraries(self):
        """Test library installation with empty libraries list."""
        cli = ClustersCLI()
        
        with pytest.raises(ValueError, match="libraries must be a non-empty list"):
            await cli.install_libraries("test-cluster-id", [])
    
    @pytest.mark.asyncio
    async def test_uninstall_libraries_success(self):
        """Test successful library uninstallation."""
        cli = ClustersCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"success": True}
            
            libraries = [
                {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}}
            ]
            
            result = await cli.uninstall_libraries("test-cluster-id", libraries)
            
            assert result["success"] is True
            assert result["cluster_id"] == "test-cluster-id"
            assert result["libraries_count"] == 1
            assert "cluster restart" in result["message"]
            
            # Verify the CLI command
            mock_execute.assert_called_once()
            args = mock_execute.call_args[0][0]
            assert args[:2] == ["libraries", "uninstall"]
    
    @pytest.mark.asyncio
    async def test_uninstall_libraries_validation(self):
        """Test uninstall libraries parameter validation."""
        cli = ClustersCLI()
        
        # Test missing cluster_id
        with pytest.raises(ValueError, match="cluster_id is required"):
            await cli.uninstall_libraries("", [])
        
        # Test empty libraries
        with pytest.raises(ValueError, match="libraries must be a non-empty list"):
            await cli.uninstall_libraries("test-cluster-id", [])
    
    @pytest.mark.asyncio
    async def test_list_cluster_libraries_success(self):
        """Test successful library listing."""
        cli = ClustersCLI()
        
        mock_response = {
            "library_statuses": [
                {
                    "library": {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}},
                    "status": "INSTALLED"
                },
                {
                    "library": {"pypi": {"package": "pandas==1.3.0"}},
                    "status": "PENDING"
                },
                {
                    "library": {"wheel": "/Workspace/path/to/library.whl"},
                    "status": "FAILED"
                }
            ]
        }
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = mock_response
            
            result = await cli.list_cluster_libraries("test-cluster-id")
            
            # Verify the result structure
            assert "library_statuses" in result
            assert "summary" in result
            
            # Verify the summary
            summary = result["summary"]
            assert summary["cluster_id"] == "test-cluster-id"
            assert summary["total_libraries"] == 3
            assert summary["installed"] == 1
            assert summary["pending"] == 1
            assert summary["failed"] == 1
            assert summary["uninstall_pending"] == 0
            
            # Verify the CLI command
            mock_execute.assert_called_once()
            args = mock_execute.call_args[0][0]
            assert args == ["libraries", "cluster-status", "test-cluster-id", "--output", "json"]
    
    @pytest.mark.asyncio
    async def test_list_cluster_libraries_empty(self):
        """Test library listing with no libraries."""
        cli = ClustersCLI()
        
        mock_response = {"library_statuses": []}
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = mock_response
            
            result = await cli.list_cluster_libraries("test-cluster-id")
            
            summary = result["summary"]
            assert summary["total_libraries"] == 0
            assert summary["installed"] == 0
    
    @pytest.mark.asyncio
    async def test_list_cluster_libraries_missing_cluster_id(self):
        """Test library listing with missing cluster_id."""
        cli = ClustersCLI()
        
        with pytest.raises(ValueError, match="cluster_id is required"):
            await cli.list_cluster_libraries("")
    
    @pytest.mark.asyncio
    async def test_library_operations_cli_error(self):
        """Test library operations handle CLI errors properly."""
        cli = ClustersCLI()
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.side_effect = CLIError("Cluster not found", [], 404)
            
            libraries = [{"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}}]
            
            with pytest.raises(CLIError):
                await cli.install_libraries("invalid-cluster-id", libraries)
    
    @pytest.mark.asyncio
    async def test_library_status_parsing(self):
        """Test library status parsing with various statuses."""
        cli = ClustersCLI()
        
        mock_response = {
            "library_statuses": [
                {"status": "INSTALLED"},
                {"status": "PENDING"},
                {"status": "INSTALL_PENDING"},  
                {"status": "FAILED"},
                {"status": "UNINSTALL_PENDING_RESTART"}
            ]
        }
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = mock_response
            
            result = await cli.list_cluster_libraries("test-cluster-id")
            
            summary = result["summary"]
            assert summary["installed"] == 1
            assert summary["pending"] == 2  # Both PENDING and INSTALL_PENDING
            assert summary["failed"] == 1
            assert summary["uninstall_pending"] == 1


class TestClusterLibraryIntegration:
    """Test integration scenarios for cluster library management."""
    
    @pytest.mark.asyncio
    async def test_install_then_list_libraries(self):
        """Test installing libraries and then listing them."""
        cli = ClustersCLI()
        
        # Mock the install operation
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"success": True}
            
            libraries = [
                {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}},
                {"pypi": {"package": "pandas==1.3.0"}}
            ]
            
            install_result = await cli.install_libraries("test-cluster-id", libraries)
            assert install_result["success"] is True
            assert install_result["libraries_count"] == 2
        
        # Mock the list operation
        list_mock_response = {
            "library_statuses": [
                {
                    "library": {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}},
                    "status": "PENDING"
                },
                {
                    "library": {"pypi": {"package": "pandas==1.3.0"}},
                    "status": "PENDING"
                }
            ]
        }
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = list_mock_response
            
            list_result = await cli.list_cluster_libraries("test-cluster-id")
            
            assert list_result["summary"]["total_libraries"] == 2
            assert list_result["summary"]["pending"] == 2
    
    @pytest.mark.asyncio
    async def test_complex_library_management_scenario(self):
        """Test a complex scenario with multiple library operations."""
        cli = ClustersCLI()
        
        # Install mixed library types
        mixed_libraries = [
            {"maven": {"coordinates": "org.apache.spark:spark-sql_2.12:3.2.0"}},
            {"pypi": {"package": "scikit-learn==1.0.0"}},
            {"wheel": "/Workspace/shared/custom-library-1.0.0-py3-none-any.whl"}
        ]
        
        with patch.object(cli, 'execute', new_callable=AsyncMock) as mock_execute:
            mock_execute.return_value = {"success": True}
            
            result = await cli.install_libraries("production-cluster", mixed_libraries)
            
            assert result["success"] is True
            assert result["cluster_id"] == "production-cluster"
            assert result["libraries_count"] == 3
            
            # Verify the JSON structure sent to CLI
            args = mock_execute.call_args[0][0]
            json_arg_index = args.index("--json") + 1
            json_payload = json.loads(args[json_arg_index])
            
            assert len(json_payload["libraries"]) == 3
            assert json_payload["libraries"][0]["maven"]["coordinates"] == "org.apache.spark:spark-sql_2.12:3.2.0"
            assert json_payload["libraries"][1]["pypi"]["package"] == "scikit-learn==1.0.0"
            assert json_payload["libraries"][2]["wheel"].endswith("custom-library-1.0.0-py3-none-any.whl")