"""Databricks client for MCP server operations."""

import os
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import JobSettings, NotebookTask, Task
from databricks.sdk.service.compute import ClusterSpec
from dotenv import load_dotenv


# Load environment variables
load_dotenv()


class DatabricksClient:
    """Client for interacting with Databricks workspace."""
    
    def __init__(self) -> None:
        """Initialize the Databricks client."""
        self.client = WorkspaceClient(
            host=os.getenv("DATABRICKS_HOST"),
            token=os.getenv("DATABRICKS_TOKEN")
        )

    def create_job(
        self,
        name: str,
        notebook_path: str,
        cluster_id: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None
    ) -> int:
        """Create a new Databricks job.
        
        Args:
            name: Name of the job
            notebook_path: Path to the notebook in the workspace
            cluster_id: ID of the cluster to run on (optional)
            parameters: Job parameters (optional)
            
        Returns:
            The created job ID
        """
        # Create the notebook task
        task = Task(
            task_key="main_task",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters=parameters or {}
            )
        )
        
        # Set up cluster configuration
        if cluster_id:
            task.existing_cluster_id = cluster_id
        else:
            # Use a default cluster configuration if no cluster ID provided
            task.new_cluster = ClusterSpec(
                spark_version="13.3.x-scala2.12",
                node_type_id="i3.xlarge",
                num_workers=1
            )
        
        # Create job settings
        job_settings = JobSettings(
            name=name,
            tasks=[task],
            max_concurrent_runs=1
        )
        
        # Create the job
        response = self.client.jobs.create(
            **job_settings.as_dict()
        )
        
        return response.job_id

    def list_jobs(self, limit: int = 25) -> List[Dict[str, Any]]:
        """List jobs in the workspace.
        
        Args:
            limit: Maximum number of jobs to return
            
        Returns:
            List of job information
        """
        jobs = self.client.jobs.list(limit=limit)
        return [job.as_dict() for job in jobs]

    def get_job(self, job_id: int) -> Dict[str, Any]:
        """Get details of a specific job.
        
        Args:
            job_id: ID of the job to retrieve
            
        Returns:
            Job details
        """
        job = self.client.jobs.get(job_id=job_id)
        return job.as_dict()

    def run_job(
        self,
        job_id: int,
        parameters: Optional[Dict[str, str]] = None
    ) -> int:
        """Trigger a job run.
        
        Args:
            job_id: ID of the job to run
            parameters: Runtime parameters for the job
            
        Returns:
            The run ID
        """
        run = self.client.jobs.run_now(
            job_id=job_id,
            notebook_params=parameters or {}
        )
        return run.run_id

    def list_clusters(self) -> List[Dict[str, Any]]:
        """List available clusters in the workspace.
        
        Returns:
            List of cluster information
        """
        clusters = self.client.clusters.list()
        return [cluster.as_dict() for cluster in clusters]

    def get_run_status(self, run_id: int) -> Dict[str, Any]:
        """Get the status of a job run.
        
        Args:
            run_id: ID of the run to check
            
        Returns:
            Run status information
        """
        run = self.client.jobs.get_run(run_id=run_id)
        return run.as_dict()