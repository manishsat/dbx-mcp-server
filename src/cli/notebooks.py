"""
Databricks workspace (notebooks) CLI operations.

This module provides functions for managing Databricks notebooks using the CLI.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from src.cli.base import DatabricksCLI

logger = logging.getLogger(__name__)


class NotebooksCLI(DatabricksCLI):
    """Databricks notebooks CLI operations."""
    
    async def list_notebooks(self, path: str = "/") -> Dict[str, Any]:
        """
        List notebooks and folders in a workspace directory.
        
        Args:
            path: Workspace path to list (default: root)
            
        Returns:
            Dictionary containing notebooks and folders
        """
        logger.info(f"Listing notebooks in path: {path}")
        
        command_args = [
            "workspace", "list", 
            path,
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def export_notebook(
        self, 
        notebook_path: str, 
        output_path: str,
        format_type: str = "SOURCE"
    ) -> Dict[str, Any]:
        """
        Export a notebook from the workspace.
        
        Args:
            notebook_path: Path to the notebook in workspace
            output_path: Local path where to save the notebook
            format_type: Export format (SOURCE, HTML, JUPYTER, DBC)
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Exporting notebook {notebook_path} to {output_path}")
        
        self.validate_required_args({
            "notebook_path": notebook_path,
            "output_path": output_path
        }, ["notebook_path", "output_path"])
        
        # Validate format
        valid_formats = ["SOURCE", "HTML", "JUPYTER", "DBC"]
        if format_type.upper() not in valid_formats:
            format_type = "SOURCE"
        
        command_args = [
            "workspace", "export",
            notebook_path,
            output_path,
            "--format", format_type.upper(),
            "--output", "json"
        ]
        
        return await self.execute(command_args)
    
    async def import_notebook(
        self,
        local_path: str,
        workspace_path: str,
        language: Optional[str] = None,
        format_type: str = "AUTO",
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Import a notebook into the workspace.
        
        Args:
            local_path: Local path to the notebook file
            workspace_path: Target path in the workspace
            language: Notebook language (PYTHON, SCALA, SQL, R)
            format_type: Import format (AUTO, SOURCE, HTML, JUPYTER, DBC)
            overwrite: Whether to overwrite existing notebook
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Importing notebook from {local_path} to {workspace_path}")
        
        self.validate_required_args({
            "local_path": local_path,
            "workspace_path": workspace_path
        }, ["local_path", "workspace_path"])
        
        # Check if local file exists
        if not os.path.exists(local_path):
            raise ValueError(f"Local file does not exist: {local_path}")
        
        command_args = [
            "workspace", "import",
            local_path,
            workspace_path,
            "--format", format_type.upper()
        ]
        
        if language:
            valid_languages = ["PYTHON", "SCALA", "SQL", "R"]
            if language.upper() in valid_languages:
                command_args.extend(["--language", language.upper()])
        
        if overwrite:
            command_args.append("--overwrite")
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def delete_notebook(self, workspace_path: str, recursive: bool = False) -> Dict[str, Any]:
        """
        Delete a notebook or directory from the workspace.
        
        Args:
            workspace_path: Path to the notebook/directory in workspace
            recursive: Whether to delete recursively (for directories)
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Deleting notebook/directory: {workspace_path}")
        
        self.validate_required_args({"workspace_path": workspace_path}, ["workspace_path"])
        
        command_args = ["workspace", "delete", workspace_path]
        
        if recursive:
            command_args.append("--recursive")
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def create_directory(self, workspace_path: str) -> Dict[str, Any]:
        """
        Create a directory in the workspace.
        
        Args:
            workspace_path: Path where to create the directory
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Creating directory: {workspace_path}")
        
        self.validate_required_args({"workspace_path": workspace_path}, ["workspace_path"])
        
        command_args = [
            "workspace", "mkdirs",
            workspace_path,
            "--output", "json"
        ]
        
        return await self.execute(command_args)
    
    async def get_notebook_info(self, workspace_path: str) -> Dict[str, Any]:
        """
        Get information about a notebook or directory.
        
        Args:
            workspace_path: Path to the notebook/directory in workspace
            
        Returns:
            Dictionary containing notebook/directory information
        """
        logger.info(f"Getting info for: {workspace_path}")
        
        self.validate_required_args({"workspace_path": workspace_path}, ["workspace_path"])
        
        # Use list command on the parent directory to get info about specific item
        parent_path = os.path.dirname(workspace_path) or "/"
        result = await self.list_notebooks(parent_path)
        
        # Find the specific item in the listing
        item_name = os.path.basename(workspace_path)
        if "objects" in result:
            for obj in result["objects"]:
                if obj.get("path", "").endswith(item_name):
                    return obj
        
        # If not found, try to list the path directly (might be a directory)
        try:
            return await self.list_notebooks(workspace_path)
        except Exception:
            raise ValueError(f"Notebook or directory not found: {workspace_path}")
    
    async def run_notebook(
        self,
        notebook_path: str,
        cluster_id: str,
        timeout_seconds: int = 3600,
        parameters: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Run a notebook and wait for completion.
        
        Args:
            notebook_path: Path to the notebook in workspace
            cluster_id: ID of the cluster to run the notebook on
            timeout_seconds: Maximum time to wait for completion
            parameters: Optional notebook parameters
            
        Returns:
            Dictionary containing run result
        """
        logger.info(f"Running notebook {notebook_path} on cluster {cluster_id}")
        
        self.validate_required_args({
            "notebook_path": notebook_path,
            "cluster_id": cluster_id
        }, ["notebook_path", "cluster_id"])
        
        command_args = [
            "workspace", "run-notebook",
            notebook_path,
            "--cluster-id", cluster_id,
            "--timeout", str(timeout_seconds),
            "--output", "json"
        ]
        
        # Add parameters if provided
        if parameters:
            import json
            params_json = json.dumps(parameters)
            command_args.extend(["--base-parameters", params_json])
        
        # This command might take a long time, so we'll increase timeout
        original_timeout = self.timeout
        try:
            self.timeout = max(timeout_seconds + 60, self.timeout)
            return await self.execute(command_args)
        finally:
            self.timeout = original_timeout


# Create a global instance for easy importing
notebooks_cli = NotebooksCLI()
