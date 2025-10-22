"""
Databricks jobs CLI operations.

This module provides functions for managing Databricks jobs using the CLI.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.cli.base import DatabricksCLI

logger = logging.getLogger(__name__)


class JobsCLI(DatabricksCLI):
    """Databricks jobs CLI operations."""
    
    async def list_jobs(self, limit: int = 25, created_by: Optional[str] = None, 
                        include_all_users: bool = False) -> Dict[str, Any]:
        """
        List Databricks jobs with optional user filtering.
        
        Args:
            limit: Maximum number of jobs to return
            created_by: Filter by creator username (None = current user, 'all' = all users)
            include_all_users: If True, includes all users' jobs (overrides created_by)
            
        Returns:
            Dictionary containing jobs data, potentially filtered by user
        """
        logger.info("Listing Databricks jobs")
        
        command_args = [
            "jobs", "list", 
            "--limit", str(limit),
            "--output", "json"
        ]
        
        # Get all jobs first
        result = await self.execute(command_args)
        
        # Apply user filtering if requested
        if not include_all_users and created_by != "all":
            result = await self._filter_jobs_by_user(result, created_by)
        
        return result
    
    async def _filter_jobs_by_user(self, jobs_data: Dict[str, Any], 
                                   target_user: Optional[str] = None) -> Dict[str, Any]:
        """
        Filter jobs by user. If target_user is None, filters to current user.
        
        Args:
            jobs_data: Raw jobs data from CLI
            target_user: Username to filter by (None = current user)
            
        Returns:
            Filtered jobs data
        """
        try:
            # Get current user if not specified
            if target_user is None:
                current_user_info = await self._get_current_user()
                target_user = current_user_info.get("userName", "")
            
            # Handle different response formats
            if isinstance(jobs_data, list):
                # Direct list of jobs
                filtered_jobs = [
                    job for job in jobs_data 
                    if job.get("creator_user_name", "").lower() == target_user.lower()
                ]
                logger.info(f"Filtered {len(jobs_data)} jobs to {len(filtered_jobs)} for user: {target_user}")
                return filtered_jobs
                
            elif isinstance(jobs_data, dict) and "jobs" in jobs_data:
                # Wrapped in jobs object  
                original_jobs = jobs_data["jobs"]
                filtered_jobs = [
                    job for job in original_jobs 
                    if job.get("creator_user_name", "").lower() == target_user.lower()
                ]
                logger.info(f"Filtered {len(original_jobs)} jobs to {len(filtered_jobs)} for user: {target_user}")
                return {"jobs": filtered_jobs}
                
            else:
                # Return as-is if format is unexpected
                logger.warning(f"Unexpected jobs data format: {type(jobs_data)}")
                return jobs_data
                
        except Exception as e:
            logger.warning(f"Error filtering jobs by user: {e}. Returning all jobs.")
            return jobs_data
    
    async def _get_current_user(self) -> Dict[str, Any]:
        """Get current user information from Databricks."""
        try:
            command_args = ["current-user", "me", "--output", "json"]
            return await self.execute(command_args)
        except Exception as e:
            logger.warning(f"Could not get current user info: {e}")
            return {"userName": "unknown"}
    
    async def get_job(self, job_id: str) -> Dict[str, Any]:
        """
        Get information about a specific job.
        
        Args:
            job_id: ID of the job to retrieve
            
        Returns:
            Dictionary containing job information
        """
        logger.info(f"Getting job information for: {job_id}")
        
        self.validate_required_args({"job_id": job_id}, ["job_id"])
        
        command_args = ["jobs", "get", job_id, "--output", "json"]  # JOB_ID as positional
        return await self.execute(command_args)
    
    async def create_job(self, job_config: Dict[str, Any], existing_cluster_name: str = None, existing_cluster_id: str = None) -> Dict[str, Any]:
        """
        Create a new job with optional existing cluster support.
        
        Args:
            job_config: Job configuration dictionary
            existing_cluster_name: Optional name of existing cluster to use
            existing_cluster_id: Optional ID of existing cluster to use
            
        Returns:
            Dictionary containing the created job information
        """
        logger.info(f"Creating job: {job_config.get('name', 'Unnamed')}")
        
        # If existing cluster specified, modify tasks to use it
        if existing_cluster_id or existing_cluster_name:
            if existing_cluster_name and not existing_cluster_id:
                # Find cluster ID by name
                from src.cli.clusters import clusters_cli
                cluster_result = await clusters_cli.find_cluster_by_name(existing_cluster_name, "RUNNING")
                if cluster_result.get("found"):
                    existing_cluster_id = cluster_result["cluster_id"]
                    logger.info(f"Found cluster '{existing_cluster_name}' with ID: {existing_cluster_id}")
                else:
                    logger.warning(f"Cluster '{existing_cluster_name}' not found, will use new_cluster configuration")
            
            # Update tasks to use existing cluster if found
            if existing_cluster_id and "tasks" in job_config:
                for task in job_config["tasks"]:
                    # Remove new_cluster and add existing_cluster_id
                    if "new_cluster" in task:
                        logger.info(f"Replacing new_cluster with existing_cluster_id: {existing_cluster_id}")
                        del task["new_cluster"]
                    task["existing_cluster_id"] = existing_cluster_id
        
        self.validate_required_args(job_config, ["name"])
        
        command_args = [
            "jobs", "create",
            "--json", json.dumps(job_config),
            "--output", "json"
        ]
        
        return await self.execute(command_args)
    
    async def update_job(self, job_id: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing job's configuration.
        
        This method handles the Databricks CLI requirement that job_id must be
        included in the JSON payload when using the --json flag, not as a positional argument.
        
        Args:
            job_id: ID of the job to update (will be added to JSON automatically)
            job_config: Updated job configuration (should NOT contain job_id)
            
        Returns:
            Dictionary containing operation result
            
        Note:
            The job_id is automatically added to the job_config before sending to CLI.
            This resolves the CLI error: "when --json flag is specified, no positional 
            arguments are required. Provide 'job_id' in your JSON input"
        """
        logger.info(f"Updating job: {job_id}")
        
        self.validate_required_args({"job_id": job_id}, ["job_id"])
        
        # Databricks CLI expects job_id and new_settings structure when using --json flag
        config_with_id = {
            "job_id": job_id,
            "new_settings": job_config
        }
        job_json = json.dumps(config_with_id)
        
        command_args = [
            "jobs", "update",
            "--json", job_json
        ]
        # Update commands don't return JSON output, so we don't expect JSON
        result = await self.execute(command_args, expect_json=False)
        # Return success message since update operations succeed silently
        return {"success": True, "message": f"Job {job_id} updated successfully"}
    
    async def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None, 
                      idempotency_token: Optional[str] = None) -> Dict[str, Any]:
        """Run a job now.
        
        Args:
            job_id: ID of the job to run
            parameters: Optional parameters for the job run
            idempotency_token: Optional idempotency token to prevent duplicate runs
            
        Returns:
            Dict with job run details
        """
        logger.info(f"Running job: {job_id}")
        
        # Build JSON payload with job_id included
        json_payload = {"job_id": int(job_id)}
        
        if parameters:
            json_payload.update(parameters)
        if idempotency_token:
            json_payload["idempotency_token"] = idempotency_token
        
        # Use only --json flag, no positional args
        command_args = [
            "jobs", "run-now",
            "--json", json.dumps(json_payload),
            "--output", "json"
        ]
        
        return await self.execute(command_args)
    
    async def cancel_job_run(self, run_id: str) -> Dict[str, Any]:
        """
        Cancel a job run.
        
        Args:
            run_id: ID of the job run to cancel
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Cancelling job run: {run_id}")
        
        self.validate_required_args({"run_id": run_id}, ["run_id"])
        
        command_args = ["jobs", "cancel-run", run_id, "--output", "json"]  # RUN_ID as positional
        return await self.execute(command_args)
    
    async def get_job_run(self, run_id: str) -> Dict[str, Any]:
        """
        Get information about a specific job run.
        
        Args:
            run_id: ID of the job run to retrieve
            
        Returns:
            Dictionary containing job run information
        """
        logger.info(f"Getting job run information for: {run_id}")
        
        self.validate_required_args({"run_id": run_id}, ["run_id"])
        
        command_args = ["jobs", "get-run", run_id, "--output", "json"]  # RUN_ID as positional
        return await self.execute(command_args)
    
    async def list_job_runs(self, job_id: Optional[str] = None, limit: int = 25) -> Dict[str, Any]:
        """
        List job runs with optional filtering.
        
        Args:
            job_id: Optional job ID to filter runs for specific job
            limit: Maximum number of runs to return
            
        Returns:
            Dictionary containing job runs data
        """
        logger.info(f"Listing job runs{' for job: ' + job_id if job_id else ''}")
        
        command_args = ["jobs", "list-runs", "--limit", str(limit), "--output", "json"]
        
        if job_id:
            command_args.extend(["--job-id", job_id])
        
        return await self.execute(command_args)
    
    async def delete_job(self, job_id: str) -> Dict[str, Any]:
        """
        Delete a job.
        
        Args:
            job_id: ID of the job to delete
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Deleting job: {job_id}")
        
        self.validate_required_args({"job_id": job_id}, ["job_id"])
        
        command_args = ["jobs", "delete", job_id, "--output", "json"]  # JOB_ID as positional
        return await self.execute(command_args)
    
    async def reset_job(self, job_id: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reset/replace all job settings.
        
        Args:
            job_id: ID of the job to reset
            job_config: Complete new job configuration
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Resetting job: {job_id}")
        
        self.validate_required_args({"job_id": job_id}, ["job_id"])
        
        job_json = json.dumps(job_config)
        
        command_args = [
            "jobs", "reset",
            job_id,  # JOB_ID as positional argument
            "--json", job_json,
            "--output", "json"
        ]
        
        return await self.execute(command_args)

    async def get_job_run_output(self, run_id: str) -> Dict[str, Any]:
        """
        Get the output and logs for a job run.
        
        This is crucial for debugging failed job runs as it provides:
        - Task execution output (first 5MB)
        - Error messages and stack traces
        - Return values from notebook tasks
        - Execution metadata
        
        Args:
            run_id: ID of the job run to get output for
            
        Returns:
            Dictionary containing run output, logs, and error information
        """
        logger.info(f"Getting job run output for: {run_id}")
        
        self.validate_required_args({"run_id": run_id}, ["run_id"])
        
        command_args = ["jobs", "get-run-output", run_id, "--output", "json"]
        return await self.execute(command_args)

    async def export_job_run(self, run_id: str, views_to_export: str = "ALL") -> Dict[str, Any]:
        """
        Export and retrieve a job run for detailed debugging.
        
        This provides comprehensive information about the job run including:
        - Complete task definitions and configurations
        - Execution details and environment information
        - Notebook code (if CODE or ALL views selected)
        - Dashboard outputs (if DASHBOARDS or ALL views selected)
        
        Args:
            run_id: ID of the job run to export
            views_to_export: Which views to export (CODE, DASHBOARDS, or ALL)
            
        Returns:
            Dictionary containing exported job run information
        """
        logger.info(f"Exporting job run: {run_id} (views: {views_to_export})")
        
        self.validate_required_args({"run_id": run_id}, ["run_id"])
        
        # Validate views_to_export parameter
        valid_views = ["CODE", "DASHBOARDS", "ALL"]
        if views_to_export.upper() not in valid_views:
            raise ValueError(f"views_to_export must be one of: {valid_views}")
        
        command_args = [
            "jobs", "export-run", run_id,
            "--views-to-export", views_to_export.upper(),
            "--output", "json"
        ]
        
        return await self.execute(command_args)


# Create a global instance for easy importing
jobs_cli = JobsCLI()