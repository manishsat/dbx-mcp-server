"""Tests for the Databricks client."""

import pytest
from unittest.mock import Mock, patch
import os
from dbx_mcp_server.databricks_client import DatabricksClient


@pytest.fixture
def mock_env():
    """Mock environment variables."""
    with patch.dict(os.environ, {
        'DATABRICKS_HOST': 'https://test.cloud.databricks.com',
        'DATABRICKS_TOKEN': 'test-token'
    }):
        yield


@pytest.fixture
def databricks_client(mock_env):
    """Create a test Databricks client."""
    with patch('dbx_mcp_server.databricks_client.WorkspaceClient') as mock_client:
        client = DatabricksClient()
        client.client = mock_client
        yield client


def test_create_job(databricks_client):
    """Test job creation."""
    # Mock the jobs.create response
    mock_response = Mock()
    mock_response.job_id = 12345
    databricks_client.client.jobs.create.return_value = mock_response
    
    # Create a job
    job_id = databricks_client.create_job(
        name="Test Job",
        notebook_path="/test/notebook",
        cluster_id="cluster-123"
    )
    
    # Verify the job was created
    assert job_id == 12345
    databricks_client.client.jobs.create.assert_called_once()


def test_list_jobs(databricks_client):
    """Test listing jobs."""
    # Mock jobs list response
    mock_job1 = Mock()
    mock_job1.as_dict.return_value = {"job_id": 1, "settings": {"name": "Job 1"}}
    mock_job2 = Mock()
    mock_job2.as_dict.return_value = {"job_id": 2, "settings": {"name": "Job 2"}}
    
    databricks_client.client.jobs.list.return_value = [mock_job1, mock_job2]
    
    # List jobs
    jobs = databricks_client.list_jobs(limit=10)
    
    # Verify results
    assert len(jobs) == 2
    assert jobs[0]["job_id"] == 1
    assert jobs[1]["job_id"] == 2
    databricks_client.client.jobs.list.assert_called_once_with(limit=10)


def test_run_job(databricks_client):
    """Test running a job.""" 
    # Mock the run_now response
    mock_response = Mock()
    mock_response.run_id = 67890
    databricks_client.client.jobs.run_now.return_value = mock_response
    
    # Run a job
    run_id = databricks_client.run_job(
        job_id=123,
        parameters={"param1": "value1"}
    )
    
    # Verify the run
    assert run_id == 67890
    databricks_client.client.jobs.run_now.assert_called_once_with(
        job_id=123,
        notebook_params={"param1": "value1"}
    )