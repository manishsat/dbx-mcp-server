"""
Tests for the MCP Server functionality.

This module tests all 31 MCP tools, tool registration, parameter validation,
and response handling.
"""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, Mock, patch
from mcp.types import TextContent, Tool

from src.mcp_server import server, handle_list_tools, handle_call_tool


class TestMCPServerTools:
    """Test MCP server tool registration and functionality."""
    
    @pytest.mark.asyncio
    async def test_handle_list_tools_returns_all_31_tools(self):
        """Test that list_tools returns all 31 registered tools."""
        result = await handle_list_tools()
        
        # Should return a list of Tool objects
        assert isinstance(result, list)
        assert len(result) == 31
        
        # Verify tool names include all expected tools
        tool_names = [tool.name for tool in result]
        
        # Cluster management tools (10 tools including library management)
        cluster_tools = ['list_clusters', 'get_cluster', 'create_cluster', 'start_cluster', 
                        'terminate_cluster', 'delete_cluster', 'find_cluster_by_name',
                        'install_libraries', 'uninstall_libraries', 'list_cluster_libraries']
        
        for tool_name in cluster_tools:
            assert tool_name in tool_names
    
    @pytest.mark.asyncio
    async def test_handle_list_tools_includes_all_domains(self):
        """Test that tools cover all 4 domains: clusters, jobs, workspace, DBFS."""
        tools = await handle_list_tools()
        tool_names = [tool.name for tool in tools]
        
        # Cluster tools (10 expected - includes 3 library management tools)
        cluster_tools = [name for name in tool_names if any(x in name for x in ['cluster', 'find_cluster_by_name', 'libraries'])]
        assert len(cluster_tools) == 10
        assert 'list_clusters' in tool_names
        assert 'create_cluster' in tool_names
        assert 'start_cluster' in tool_names
        assert 'terminate_cluster' in tool_names
        assert 'delete_cluster' in tool_names
        assert 'get_cluster' in tool_names
        assert 'find_cluster_by_name' in tool_names
        # Library management tools
        assert 'install_libraries' in tool_names
        assert 'uninstall_libraries' in tool_names
        assert 'list_cluster_libraries' in tool_names
        
        # Job tools (9 expected)
        job_tools = [name for name in tool_names if 'job' in name]
        assert len(job_tools) == 9
        assert 'list_jobs' in tool_names
        assert 'create_job' in tool_names
        assert 'run_job' in tool_names
        assert 'get_job' in tool_names
        assert 'delete_job' in tool_names
        assert 'update_job' in tool_names
        assert 'cancel_job_run' in tool_names
        assert 'get_job_run' in tool_names
        assert 'list_job_runs' in tool_names
        
        # Workspace tools (8 expected)
        workspace_tools = [name for name in tool_names if any(x in name for x in ['workspace', 'notebook', 'user_workspace'])]
        assert len(workspace_tools) == 8
        assert 'list_workspace' in tool_names
        assert 'get_workspace_item' in tool_names
        assert 'create_notebook' in tool_names
        assert 'upload_notebook' in tool_names
        assert 'delete_workspace_item' in tool_names
        assert 'create_workspace_directory' in tool_names
        assert 'get_user_workspace_path' in tool_names
        assert 'setup_user_workspace' in tool_names
        
        # DBFS tools (4 expected)
        dbfs_tools = [name for name in tool_names if 'file' in name]
        assert len(dbfs_tools) == 4
        assert 'list_files' in tool_names
        assert 'upload_file' in tool_names
        assert 'download_file' in tool_names
        assert 'delete_file' in tool_names


class TestMCPToolHandlers:
    """Test individual MCP tool handlers."""
    
    @pytest.mark.asyncio
    async def test_list_clusters_handler_success(self):
        """Test list_clusters tool handler success case."""
        mock_clusters_data = [{"cluster_id": "test-123", "cluster_name": "test-cluster"}]
        
        with patch('src.mcp_server.clusters_cli.list_clusters', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_clusters_data
            
            result = await handle_call_tool("list_clusters", {})
            
            assert isinstance(result, list)
            assert len(result) == 1
            assert isinstance(result[0], TextContent)
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_clusters_data
            mock_cli.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_cluster_handler_success(self):
        """Test get_cluster tool handler with valid cluster_id."""
        cluster_id = "test-cluster-123"
        mock_cluster_data = {"cluster_id": cluster_id, "state": "RUNNING"}
        
        with patch('src.mcp_server.clusters_cli.get_cluster', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_cluster_data
            
            result = await handle_call_tool("get_cluster", {"cluster_id": cluster_id})
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_cluster_data
            mock_cli.assert_called_once_with(cluster_id)
    
    @pytest.mark.asyncio
    async def test_get_cluster_handler_missing_cluster_id(self):
        """Test get_cluster tool handler with missing cluster_id."""
        result = await handle_call_tool("get_cluster", {})
        
        response_data = json.loads(result[0].text)
        assert "error" in response_data
        assert "cluster_id is required" in response_data["error"]
        assert response_data["tool"] == "get_cluster"
    
    @pytest.mark.asyncio
    async def test_create_cluster_handler_success(self):
        """Test create_cluster tool handler with valid parameters."""
        cluster_config = {
            "cluster_name": "test-cluster",
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2
        }
        mock_response = {"cluster_id": "new-cluster-123"}
        
        with patch('src.mcp_server.clusters_cli.create_cluster', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_response
            
            result = await handle_call_tool("create_cluster", cluster_config)
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_response
            
            # Verify the CLI was called with correct config
            call_args = mock_cli.call_args[0][0]
            assert call_args["cluster_name"] == "test-cluster"
            assert call_args["spark_version"] == "13.3.x-scala2.12"
            assert call_args["node_type_id"] == "i3.xlarge"
            assert call_args["num_workers"] == 2
    
    @pytest.mark.asyncio
    async def test_create_cluster_handler_missing_required_params(self):
        """Test create_cluster tool handler with missing required parameters."""
        # Missing spark_version and node_type_id
        incomplete_config = {"cluster_name": "test-cluster"}
        
        result = await handle_call_tool("create_cluster", incomplete_config)
        
        response_data = json.loads(result[0].text)
        assert "error" in response_data
        assert "required" in response_data["error"].lower()
    
    @pytest.mark.asyncio
    async def test_install_libraries_handler_success(self):
        """Test install_libraries tool handler success case."""
        cluster_id = "test-cluster-123"
        libraries = [{"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}}]
        mock_result = {
            "success": True,
            "cluster_id": cluster_id,
            "libraries_count": 1,
            "message": "Library installation initiated",
            "libraries": libraries
        }
        
        with patch('src.mcp_server.clusters_cli.install_libraries', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_result
            
            result = await handle_call_tool("install_libraries", {
                "cluster_id": cluster_id,
                "libraries": libraries
            })
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_result
            mock_cli.assert_called_once_with(cluster_id, libraries)
    
    @pytest.mark.asyncio
    async def test_install_libraries_handler_missing_params(self):
        """Test install_libraries tool handler with missing parameters."""
        # Test missing cluster_id
        result = await handle_call_tool("install_libraries", {"libraries": []})
        response_data = json.loads(result[0].text)
        assert "error" in response_data
        assert "cluster_id is required" in response_data["error"]
        
        # Test missing libraries
        result = await handle_call_tool("install_libraries", {"cluster_id": "test"})
        response_data = json.loads(result[0].text)
        assert "error" in response_data
        assert "libraries is required" in response_data["error"]
    
    @pytest.mark.asyncio
    async def test_uninstall_libraries_handler_success(self):
        """Test uninstall_libraries tool handler success case."""
        cluster_id = "test-cluster-123"
        libraries = [{"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}}]
        mock_result = {
            "success": True,
            "cluster_id": cluster_id,
            "libraries_count": 1,
            "message": "Library uninstallation initiated",
            "libraries": libraries
        }
        
        with patch('src.mcp_server.clusters_cli.uninstall_libraries', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_result
            
            result = await handle_call_tool("uninstall_libraries", {
                "cluster_id": cluster_id,
                "libraries": libraries
            })
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_result
            mock_cli.assert_called_once_with(cluster_id, libraries)
    
    @pytest.mark.asyncio
    async def test_list_cluster_libraries_handler_success(self):
        """Test list_cluster_libraries tool handler success case."""
        cluster_id = "test-cluster-123"
        mock_result = {
            "library_statuses": [
                {"library": {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}}, "status": "INSTALLED"}
            ],
            "summary": {
                "cluster_id": cluster_id,
                "total_libraries": 1,
                "installed": 1,
                "pending": 0,
                "failed": 0,
                "uninstall_pending": 0
            }
        }
        
        with patch('src.mcp_server.clusters_cli.list_cluster_libraries', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_result
            
            result = await handle_call_tool("list_cluster_libraries", {"cluster_id": cluster_id})
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_result
            mock_cli.assert_called_once_with(cluster_id)
    
    @pytest.mark.asyncio
    async def test_list_cluster_libraries_handler_missing_cluster_id(self):
        """Test list_cluster_libraries tool handler with missing cluster_id."""
        result = await handle_call_tool("list_cluster_libraries", {})
        
        response_data = json.loads(result[0].text)
        assert "error" in response_data
        assert "cluster_id is required" in response_data["error"]
    
    @pytest.mark.asyncio
    async def test_list_jobs_handler_success(self):
        """Test list_jobs tool handler success case."""
        mock_jobs_data = {"jobs": [{"job_id": 123, "settings": {"name": "test-job"}}]}
        
        with patch('src.mcp_server.jobs_cli.list_jobs', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_jobs_data
            
            result = await handle_call_tool("list_jobs", {"limit": 10})
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_jobs_data
            mock_cli.assert_called_once_with(10, None, False)
    
    @pytest.mark.asyncio
    async def test_run_job_handler_success(self):
        """Test run_job tool handler with valid job_id."""
        job_id = "123"
        mock_run_data = {"run_id": 456, "state": {"state_message": "PENDING"}}
        
        with patch('src.mcp_server.jobs_cli.run_job', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_run_data
            
            result = await handle_call_tool("run_job", {"job_id": job_id})
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_run_data
            mock_cli.assert_called_once_with(job_id, None, None)
    
    @pytest.mark.asyncio
    async def test_list_workspace_handler_success(self):
        """Test list_workspace tool handler success case."""
        mock_workspace_data = [{"path": "/test", "object_type": "DIRECTORY"}]
        
        with patch('src.mcp_server.workspace_cli.list_workspace_items', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_workspace_data
            
            result = await handle_call_tool("list_workspace", {"path": "/test"})
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_workspace_data
            mock_cli.assert_called_once_with("/test", False)
    
    @pytest.mark.asyncio
    async def test_list_files_handler_success(self):
        """Test list_files (DBFS) tool handler success case."""
        mock_files_data = {"path": "/", "files": [{"path": "/test.txt", "is_dir": False}]}
        
        with patch('src.mcp_server.dbfs_cli.list_files', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_files_data
            
            result = await handle_call_tool("list_files", {"dbfs_path": "/"})
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_files_data
            mock_cli.assert_called_once_with("/")
    
    @pytest.mark.asyncio
    async def test_upload_file_handler_success(self):
        """Test upload_file (DBFS) tool handler success case."""
        mock_upload_data = {"status": "success", "path": "/test.txt"}
        
        with patch('src.mcp_server.dbfs_cli.upload_file', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_upload_data
            
            result = await handle_call_tool("upload_file", {
                "local_path": "/tmp/test.txt",
                "dbfs_path": "/test.txt",
                "overwrite": True
            })
            
            response_data = json.loads(result[0].text)
            assert response_data == mock_upload_data
            mock_cli.assert_called_once_with("/tmp/test.txt", "/test.txt", True)
    
    @pytest.mark.asyncio
    async def test_unknown_tool_handler(self):
        """Test handler for unknown tool name."""
        result = await handle_call_tool("unknown_tool", {})
        
        response_data = json.loads(result[0].text)
        assert "error" in response_data
        assert "Unknown tool: unknown_tool" in response_data["error"]
        assert response_data["tool"] == "unknown_tool"
    
    @pytest.mark.asyncio
    async def test_handler_cli_exception(self):
        """Test handler when CLI raises exception."""
        with patch('src.mcp_server.clusters_cli.list_clusters', new_callable=AsyncMock) as mock_cli:
            mock_cli.side_effect = Exception("CLI connection failed")
            
            result = await handle_call_tool("list_clusters", {})
            
            response_data = json.loads(result[0].text)
            assert "error" in response_data
            assert "CLI connection failed" in response_data["error"]
            assert response_data["tool"] == "list_clusters"


class TestMCPServerConfiguration:
    """Test MCP server configuration and initialization."""
    
    def test_server_instance_creation(self):
        """Test that server instance is created correctly."""
        from src.mcp_server import server
        
        assert server is not None
        assert server.name == "databricks-mcp"
    
    @pytest.mark.asyncio
    async def test_tool_schemas_validation(self):
        """Test that all tool schemas are valid JSON Schema format."""
        tools = await handle_list_tools()
        
        for tool in tools:
            schema = tool.inputSchema
            
            # Basic schema validation
            assert isinstance(schema, dict)
            assert "type" in schema
            assert schema["type"] == "object"
            assert "properties" in schema
            assert "additionalProperties" in schema
            assert schema["additionalProperties"] is False
            
            # Validate required fields if present
            if "required" in schema:
                assert isinstance(schema["required"], list)
                for req_field in schema["required"]:
                    assert req_field in schema["properties"]


class TestMCPResponseFormatting:
    """Test MCP response formatting and JSON serialization."""
    
    @pytest.mark.asyncio
    async def test_successful_response_format(self):
        """Test that successful responses are properly formatted."""
        mock_data = {"test": "data", "number": 123}
        
        with patch('src.mcp_server.clusters_cli.list_clusters', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = mock_data
            
            result = await handle_call_tool("list_clusters", {})
            
            assert isinstance(result, list)
            assert len(result) == 1
            assert isinstance(result[0], TextContent)
            assert result[0].type == "text"
            
            # Verify JSON format
            parsed_data = json.loads(result[0].text)
            assert parsed_data == mock_data
    
    @pytest.mark.asyncio
    async def test_error_response_format(self):
        """Test that error responses are properly formatted."""
        result = await handle_call_tool("get_cluster", {})  # Missing required param
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], TextContent)
        
        parsed_data = json.loads(result[0].text)
        assert "error" in parsed_data
        assert "tool" in parsed_data
        assert "arguments" in parsed_data
        assert parsed_data["tool"] == "get_cluster"
        assert parsed_data["arguments"] == {}
    
    @pytest.mark.asyncio
    async def test_complex_data_serialization(self):
        """Test that complex data structures are properly serialized."""
        complex_data = {
            "nested": {"key": "value"},
            "array": [1, 2, 3],
            "null_value": None,
            "boolean": True
        }
        
        with patch('src.mcp_server.clusters_cli.list_clusters', new_callable=AsyncMock) as mock_cli:
            mock_cli.return_value = complex_data
            
            result = await handle_call_tool("list_clusters", {})
            
            parsed_data = json.loads(result[0].text)
            assert parsed_data == complex_data