"""
Databricks Clusters CLI operations.

This module provides functions for managing Databricks clusters using the CLI.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.cli.base import DatabricksCLI

logger = logging.getLogger(__name__)


class ClustersCLI(DatabricksCLI):
    """Databricks clusters CLI operations."""
    
    async def list_clusters(self) -> Dict[str, Any]:
        """
        List all Databricks clusters.
        
        Returns:
            Dictionary containing clusters data
        """
        logger.info("Listing Databricks clusters")
        
        command_args = ["clusters", "list", "--output", "json"]
        return await self.execute(command_args)
    
    async def get_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Get information about a specific cluster.
        
        Args:
            cluster_id: ID of the cluster to retrieve
            
        Returns:
            Dictionary containing cluster information
        """
        logger.info(f"Getting cluster information for: {cluster_id}")
        
        self.validate_required_args({"cluster_id": cluster_id}, ["cluster_id"])
        
        command_args = ["clusters", "get", cluster_id, "--output", "json"]
        return await self.execute(command_args)
    
    async def create_cluster(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new Databricks cluster.
        
        Args:
            cluster_config: Cluster configuration dictionary
            
        Returns:
            Dictionary containing created cluster information
        """
        logger.info(f"Creating cluster: {cluster_config.get('cluster_name', 'unnamed')}")
        
        # Validate required fields
        required_fields = ["cluster_name", "spark_version", "node_type_id"]
        self.validate_required_args(cluster_config, required_fields)
        
        # For complex configurations or autoscaling, use JSON approach
        if "autoscale" in cluster_config or len(cluster_config) > 5:
            # Use JSON config approach for complex configurations
            command_args = [
                "clusters", "create", 
                cluster_config["spark_version"],  # Positional argument
                "--json", json.dumps(cluster_config),
                "--output", "json"
            ]
        else:
            # Use flag-based approach for simple configurations
            command_args = [
                "clusters", "create",
                cluster_config["spark_version"],  # Positional argument
                "--cluster-name", cluster_config["cluster_name"],
                "--node-type-id", cluster_config["node_type_id"],
                "--output", "json"
            ]
            
            # Add optional parameters
            if "num_workers" in cluster_config:
                command_args.extend(["--num-workers", str(cluster_config["num_workers"])])
            else:
                # Default to single node if not specified
                command_args.extend(["--num-workers", "1"])
            
            if "autotermination_minutes" in cluster_config:
                command_args.extend([
                    "--autotermination-minutes", 
                    str(cluster_config["autotermination_minutes"])
                ])
            
            if "driver_node_type_id" in cluster_config:
                command_args.extend([
                    "--driver-node-type-id", 
                    cluster_config["driver_node_type_id"]
                ])
                
            if cluster_config.get("enable_elastic_disk"):
                command_args.append("--enable-elastic-disk")
                
            if cluster_config.get("enable_local_disk_encryption"):
                command_args.append("--enable-local-disk-encryption")
        
        return await self.execute(command_args)
    
    async def start_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Start a terminated cluster.
        
        Args:
            cluster_id: ID of the cluster to start
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Starting cluster: {cluster_id}")
        
        self.validate_required_args({"cluster_id": cluster_id}, ["cluster_id"])
        
        command_args = ["clusters", "start", cluster_id, "--output", "json"]
        return await self.execute(command_args)
    
    async def terminate_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Terminate a running cluster.
        
        Args:
            cluster_id: ID of the cluster to terminate
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Terminating cluster: {cluster_id}")
        
        self.validate_required_args({"cluster_id": cluster_id}, ["cluster_id"])
        
        command_args = ["clusters", "delete", cluster_id, "--output", "json"]
        return await self.execute(command_args)
    
    async def delete_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Permanently delete a cluster.
        
        Args:
            cluster_id: ID of the cluster to permanently delete
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Permanently deleting cluster: {cluster_id}")
        
        self.validate_required_args({"cluster_id": cluster_id}, ["cluster_id"])
        
        command_args = ["clusters", "permanent-delete", cluster_id, "--output", "json"]
        return await self.execute(command_args)
    
    async def restart_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """
        Restart a cluster.
        
        Args:
            cluster_id: ID of the cluster to restart
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Restarting cluster: {cluster_id}")
        
        self.validate_required_args({"cluster_id": cluster_id}, ["cluster_id"])
        
        command_args = ["clusters", "restart", cluster_id, "--output", "json"]
        return await self.execute(command_args)
    
    async def find_cluster_by_name(self, cluster_name: str, state_filter: str = "RUNNING") -> Dict[str, Any]:
        """
        Find cluster ID by name, optionally filtering by state.
        
        Args:
            cluster_name: Name of the cluster to find
            state_filter: Optional state filter (RUNNING, TERMINATED, etc.)
            
        Returns:
            Dictionary containing cluster info or None if not found
        """
        logger.info(f"Finding cluster by name: {cluster_name} (state: {state_filter})")
        
        # Get all clusters
        clusters = await self.list_clusters()
        
        # Search for cluster by name
        matching_clusters = []
        for cluster in clusters:
            if cluster.get("cluster_name") == cluster_name:
                cluster_state = cluster.get("state", "UNKNOWN")
                if state_filter == "ALL" or cluster_state == state_filter:
                    matching_clusters.append(cluster)
        
        if not matching_clusters:
            return {
                "found": False,
                "message": f"No cluster named '{cluster_name}' found with state '{state_filter}'",
                "available_states": [c.get("state") for c in clusters if c.get("cluster_name") == cluster_name]
            }
        
        # Return the first matching cluster (or best match if multiple)
        best_cluster = matching_clusters[0]
        if len(matching_clusters) > 1:
            # Prefer RUNNING clusters
            for cluster in matching_clusters:
                if cluster.get("state") == "RUNNING":
                    best_cluster = cluster
                    break
        
        return {
            "found": True,
            "cluster_id": best_cluster.get("cluster_id"),
            "cluster_name": best_cluster.get("cluster_name"),
            "state": best_cluster.get("state"),
            "node_type_id": best_cluster.get("node_type_id"),
            "spark_version": best_cluster.get("spark_version"),
            "num_workers": best_cluster.get("num_workers", 0),
            "total_matches": len(matching_clusters)
        }

    async def install_libraries(
        self, 
        cluster_id: str, 
        libraries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Install libraries on a Databricks cluster.
        
        Args:
            cluster_id: Target cluster ID
            libraries: List of library specifications (maven, wheel, pypi, etc.)
        
        Returns:
            Dictionary containing installation result
            
        Example libraries format:
        [
            {"maven": {"coordinates": "org.jsoup:jsoup:1.7.2"}},
            {"wheel": "/Workspace/path/to/library.whl"},
            {"pypi": {"package": "pandas==1.3.0"}}
        ]
        """
        logger.info(f"Installing libraries on cluster {cluster_id}")
        
        # Validate cluster_id
        if not cluster_id:
            raise ValueError("cluster_id is required")
            
        # Validate libraries
        if not libraries or not isinstance(libraries, list):
            raise ValueError("libraries must be a non-empty list")
        
        # Prepare the request body
        request_body = {
            "cluster_id": cluster_id,
            "libraries": libraries
        }
        
        command_args = [
            "libraries", "install",
            "--json", json.dumps(request_body),
            "--output", "json"
        ]
        
        result = await self.execute(command_args, expect_json=False)
        
        return {
            "success": True,
            "cluster_id": cluster_id,
            "libraries_count": len(libraries),
            "message": "Library installation initiated (asynchronous operation)",
            "libraries": libraries
        }

    async def uninstall_libraries(
        self, 
        cluster_id: str, 
        libraries: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Uninstall libraries from a Databricks cluster.
        
        Args:
            cluster_id: Target cluster ID
            libraries: List of library specifications to uninstall
        
        Returns:
            Dictionary containing uninstallation result
        """
        logger.info(f"Uninstalling libraries from cluster {cluster_id}")
        
        # Validate cluster_id
        if not cluster_id:
            raise ValueError("cluster_id is required")
            
        # Validate libraries
        if not libraries or not isinstance(libraries, list):
            raise ValueError("libraries must be a non-empty list")
        
        # Prepare the request body
        request_body = {
            "cluster_id": cluster_id,
            "libraries": libraries
        }
        
        command_args = [
            "libraries", "uninstall",
            "--json", json.dumps(request_body),
            "--output", "json"
        ]
        
        result = await self.execute(command_args, expect_json=False)
        
        return {
            "success": True,
            "cluster_id": cluster_id,
            "libraries_count": len(libraries),
            "message": "Library uninstallation initiated (requires cluster restart)",
            "libraries": libraries
        }

    async def list_cluster_libraries(self, cluster_id: str) -> Dict[str, Any]:
        """
        List all libraries installed on a Databricks cluster.
        
        Args:
            cluster_id: Cluster ID to query
        
        Returns:
            Dictionary containing library status information
        """
        logger.info(f"Listing libraries for cluster {cluster_id}")
        
        # Validate cluster_id
        if not cluster_id:
            raise ValueError("cluster_id is required")
        
        command_args = [
            "libraries", "cluster-status", cluster_id,
            "--output", "json"
        ]
        
        result = await self.execute(command_args)
        
        # Parse and enhance the response
        # Handle the CLI response format - it returns a list directly
        if isinstance(result, list):
            library_statuses = result
        else:
            library_statuses = result.get("library_statuses", [])
        
        summary = {
            "cluster_id": cluster_id,
            "total_libraries": len(library_statuses),
            "installed": 0,
            "pending": 0,
            "failed": 0,
            "uninstall_pending": 0
        }
        
        for lib_status in library_statuses:
            status = lib_status.get("status", "").lower()
            if "installed" in status:
                summary["installed"] += 1
            elif "uninstall" in status:
                summary["uninstall_pending"] += 1
            elif "pending" in status:
                summary["pending"] += 1
            elif "failed" in status:
                summary["failed"] += 1
        
        return {
            "library_statuses": library_statuses,
            "summary": summary
        }


# Create a global instance for easy importing
clusters_cli = ClustersCLI()
