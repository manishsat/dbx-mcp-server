"""Tests for the Databricks MCP Server."""

import pytest
from unittest.mock import Mock, patch


@pytest.mark.asyncio
@patch('dbx_mcp_server.main.DatabricksClient')
async def test_create_job_functionality(mock_client_class):
    """Test the create_job functionality."""
    # Mock the client
    mock_client = Mock()
    mock_client.create_job.return_value = 12345
    mock_client_class.return_value = mock_client
    
    from dbx_mcp_server.main import DatabricksMCPServer
    server = DatabricksMCPServer()
    
    # Test the create_job method directly
    result = await server._create_job({
        "name": "Test Job",
        "notebook_path": "/test/notebook",
        "cluster_id": "test-cluster-123"
    })
    
    assert "Successfully created job 'Test Job' with ID: 12345" in result
    mock_client.create_job.assert_called_once()


@pytest.mark.asyncio
@patch('dbx_mcp_server.main.DatabricksClient')
async def test_list_jobs_functionality(mock_client_class):
    """Test the list_jobs functionality."""
    # Mock the client
    mock_client = Mock()
    mock_client.list_jobs.return_value = [
        {"job_id": 1, "settings": {"name": "Job 1"}},
        {"job_id": 2, "settings": {"name": "Job 2"}}
    ]
    mock_client_class.return_value = mock_client
    
    from dbx_mcp_server.main import DatabricksMCPServer
    server = DatabricksMCPServer()
    
    # Test the list_jobs method directly
    result = await server._list_jobs({})
    
    assert "Job 1" in result
    assert "Job 2" in result
    mock_client.list_jobs.assert_called_once()


@pytest.mark.asyncio
@patch('dbx_mcp_server.main.DatabricksClient') 
async def test_run_job_functionality(mock_client_class):
    """Test the run_job functionality."""
    # Mock the client
    mock_client = Mock()
    mock_client.run_job.return_value = 67890
    mock_client_class.return_value = mock_client
    
    from dbx_mcp_server.main import DatabricksMCPServer
    server = DatabricksMCPServer()
    
    # Test the run_job method directly
    result = await server._run_job({
        "job_id": 123,
        "parameters": {"param1": "value1"}
    })
    
    assert "Successfully triggered job 123. Run ID: 67890" in result
    mock_client.run_job.assert_called_once_with(
        job_id=123, 
        parameters={"param1": "value1"}
    )


@pytest.mark.asyncio
@patch('dbx_mcp_server.main.DatabricksClient')
async def test_list_clusters_functionality(mock_client_class):
    """Test the list_clusters functionality."""
    # Mock the client
    mock_client = Mock()
    mock_client.list_clusters.return_value = [
        {"cluster_id": "cluster-1", "cluster_name": "Test Cluster 1", "state": "RUNNING"},
        {"cluster_id": "cluster-2", "cluster_name": "Test Cluster 2", "state": "TERMINATED"}
    ]
    mock_client_class.return_value = mock_client
    
    from dbx_mcp_server.main import DatabricksMCPServer
    server = DatabricksMCPServer()
    
    # Test the list_clusters method directly
    result = await server._list_clusters({})
    
    assert "Test Cluster 1" in result
    assert "Test Cluster 2" in result
    assert "RUNNING" in result
    assert "TERMINATED" in result
    mock_client.list_clusters.assert_called_once()