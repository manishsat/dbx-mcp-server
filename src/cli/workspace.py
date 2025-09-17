"""
Databricks Workspace CLI operations for MCP server.

Handles notebook upload, workspace management, and file operations.
"""

import json
import logging
import tempfile
import os
from typing import Dict, List, Optional, Any

from src.cli.base import DatabricksCLI

logger = logging.getLogger(__name__)


class WorkspaceCLI(DatabricksCLI):
    """Databricks workspace operations CLI interface."""
    
    async def list_workspace_items(self, path: str = "/", recursive: bool = False, max_depth: int = 1) -> List[Dict[str, Any]]:
        """List items in workspace path."""
        logger.info(f"Listing workspace items at path: {path}")
        
        command_args = [
            "workspace", "list",
            path,
            "--output", "json"
        ]
        
        if recursive:
            command_args.extend(["--recursive"])
        
        result = await self.execute(command_args)
        
        # Parse the result - it should be a list of workspace items
        if isinstance(result, list):
            return result
        else:
            # Handle case where result is not a list
            return []
    
    async def get_workspace_item(self, path: str) -> Dict[str, Any]:
        """Get details of a specific workspace item."""
        logger.info(f"Getting workspace item: {path}")
        
        command_args = [
            "workspace", "get-status",
            path,
            "--output", "json"
        ]
        
        return await self.execute(command_args)
    
    async def create_notebook(self, path: str, content: str, language: str = "PYTHON", format_type: str = "SOURCE") -> Dict[str, Any]:
        """Create a notebook in the workspace with given content."""
        logger.info(f"Creating notebook at path: {path}")
        
        # Create a temporary file with the notebook content
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as temp_file:
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        try:
            command_args = [
                "workspace", "import", path,
                "--file", temp_file_path,
                "--language", language.upper(),
                "--format", format_type.upper(),
                "--overwrite"
            ]
            
            result = await self.execute(command_args)
            
            # The import command typically returns success without output
            return {
                "path": path,
                "language": language,
                "format": format_type,
                "status": "created"
            }
            
        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_file_path)
            except OSError:
                pass
    
    async def upload_notebook(self, local_path: str, workspace_path: str, language: str = "PYTHON") -> Dict[str, Any]:
        """Upload a local notebook file to workspace."""
        logger.info(f"Uploading notebook from {local_path} to {workspace_path}")
        
        command_args = [
            "workspace", "import", workspace_path,
            "--file", local_path,
            "--language", language.upper(),
            "--format", "SOURCE",
            "--overwrite"
        ]
        
        await self.execute(command_args)
        
        return {
            "local_path": local_path,
            "workspace_path": workspace_path,
            "language": language,
            "status": "uploaded"
        }
    
    async def delete_workspace_item(self, path: str, recursive: bool = False) -> Dict[str, Any]:
        """Delete a workspace item (notebook, folder, etc.)."""
        logger.info(f"Deleting workspace item: {path}")
        
        command_args = [
            "workspace", "delete",
            path
        ]
        
        if recursive:
            command_args.extend(["--recursive"])
        
        await self.execute(command_args)
        
        return {
            "path": path,
            "status": "deleted",
            "recursive": recursive
        }
    
    async def create_directory(self, path: str) -> Dict[str, Any]:
        """Create a directory in the workspace."""
        logger.info(f"Creating workspace directory: {path}")
        
        command_args = [
            "workspace", "mkdirs",
            path
        ]
        
        await self.execute(command_args)
        
        return {
            "path": path,
            "status": "created",
            "type": "directory"
        }
    
    async def export_notebook(self, workspace_path: str, local_path: str, format_type: str = "SOURCE") -> Dict[str, Any]:
        """Export a notebook from workspace to local file."""
        logger.info(f"Exporting notebook from {workspace_path} to {local_path}")
        
        command_args = [
            "workspace", "export",
            workspace_path,
            local_path,
            "--format", format_type.upper()
        ]
        
        await self.execute(command_args)
        
        return {
            "workspace_path": workspace_path,
            "local_path": local_path,
            "format": format_type,
            "status": "exported"
        }
    
    async def get_current_user_info(self) -> Dict[str, Any]:
        """Get current user information for workspace path construction."""
        logger.info("Getting current user information")
        
        command_args = [
            "current-user", "me",
            "--output", "json"
        ]
        
        return await self.execute(command_args)
    
    async def get_user_workspace_path(self) -> str:
        """Get the current user's workspace path."""
        user_info = await self.get_current_user_info()
        username = user_info.get("userName", "unknown_user")
        return f"/Workspace/Users/{username}"
    
    async def create_user_directory_structure(self, subdirs: List[str] = None) -> Dict[str, Any]:
        """Create directory structure in user's workspace."""
        user_path = await self.get_user_workspace_path()
        
        created_dirs = []
        
        # Create base user directory
        try:
            await self.create_directory(user_path)
            created_dirs.append(user_path)
        except Exception as e:
            logger.info(f"User directory {user_path} may already exist: {e}")
        
        # Create subdirectories if specified
        if subdirs:
            for subdir in subdirs:
                full_path = f"{user_path}/{subdir}"
                try:
                    await self.create_directory(full_path)
                    created_dirs.append(full_path)
                except Exception as e:
                    logger.info(f"Directory {full_path} may already exist: {e}")
        
        return {
            "user_path": user_path,
            "created_directories": created_dirs,
            "status": "initialized"
        }


# Global instance
workspace_cli = WorkspaceCLI()
