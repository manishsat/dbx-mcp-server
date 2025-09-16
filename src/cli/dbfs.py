"""
Databricks File System (DBFS) CLI operations.

This module provides functions for managing files in DBFS using the CLI.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from src.cli.base import DatabricksCLI

logger = logging.getLogger(__name__)


class DBFSCLI(DatabricksCLI):
    """Databricks File System CLI operations."""
    
    async def list_files(self, dbfs_path: str = "/") -> Dict[str, Any]:
        """
        List files and directories in a DBFS path.
        
        Args:
            dbfs_path: DBFS path to list (default: root)
            
        Returns:
            Dictionary containing files and directories
        """
        logger.info(f"Listing files in DBFS path: {dbfs_path}")
        
        # Ensure path starts with /
        if not dbfs_path.startswith("/"):
            dbfs_path = "/" + dbfs_path
        
        command_args = [
            "fs", "ls",
            dbfs_path,
            "--output", "json"
        ]
        raw_result = await self.execute(command_args)
        
        # The CLI returns a list directly, wrap it in a dictionary for consistency
        if isinstance(raw_result, list):
            return {
                "path": dbfs_path,
                "files": raw_result
            }
        
        return raw_result
    
    async def upload_file(
        self,
        local_path: str,
        dbfs_path: str,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Upload a file to DBFS.
        
        Args:
            local_path: Local path to the file to upload
            dbfs_path: Target DBFS path
            overwrite: Whether to overwrite existing file
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Uploading file from {local_path} to {dbfs_path}")
        
        self.validate_required_args({
            "local_path": local_path,
            "dbfs_path": dbfs_path
        }, ["local_path", "dbfs_path"])
        
        # Check if local file exists
        if not os.path.exists(local_path):
            raise ValueError(f"Local file does not exist: {local_path}")
        
        # Ensure DBFS path starts with /
        if not dbfs_path.startswith("/"):
            dbfs_path = "/" + dbfs_path
        
        command_args = [
            "fs", "cp",
            local_path,
            f"dbfs:{dbfs_path}"
        ]
        
        if overwrite:
            command_args.append("--overwrite")
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def download_file(
        self,
        dbfs_path: str,
        local_path: str,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Download a file from DBFS.
        
        Args:
            dbfs_path: DBFS path to the file to download
            local_path: Local path where to save the file
            overwrite: Whether to overwrite existing local file
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Downloading file from {dbfs_path} to {local_path}")
        
        self.validate_required_args({
            "dbfs_path": dbfs_path,
            "local_path": local_path
        }, ["dbfs_path", "local_path"])
        
        # Ensure DBFS path starts with /
        if not dbfs_path.startswith("/"):
            dbfs_path = "/" + dbfs_path
        
        # Create local directory if it doesn't exist
        local_dir = os.path.dirname(local_path)
        if local_dir and not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)
        
        command_args = [
            "fs", "cp",
            f"dbfs:{dbfs_path}",
            local_path
        ]
        
        if overwrite:
            command_args.append("--overwrite")
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def delete_file(self, dbfs_path: str, recursive: bool = False) -> Dict[str, Any]:
        """
        Delete a file or directory from DBFS.
        
        Args:
            dbfs_path: DBFS path to delete
            recursive: Whether to delete recursively (for directories)
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Deleting DBFS path: {dbfs_path}")
        
        self.validate_required_args({"dbfs_path": dbfs_path}, ["dbfs_path"])
        
        # Ensure DBFS path starts with /
        if not dbfs_path.startswith("/"):
            dbfs_path = "/" + dbfs_path
        
        command_args = [
            "fs", "rm",
            f"dbfs:{dbfs_path}"
        ]
        
        if recursive:
            command_args.append("--recursive")
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def create_directory(self, dbfs_path: str) -> Dict[str, Any]:
        """
        Create a directory in DBFS.
        
        Args:
            dbfs_path: DBFS path where to create the directory
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Creating DBFS directory: {dbfs_path}")
        
        self.validate_required_args({"dbfs_path": dbfs_path}, ["dbfs_path"])
        
        # Ensure DBFS path starts with /
        if not dbfs_path.startswith("/"):
            dbfs_path = "/" + dbfs_path
        
        command_args = [
            "fs", "mkdirs",
            f"dbfs:{dbfs_path}",
            "--output", "json"
        ]
        
        return await self.execute(command_args)
    
    async def move_file(
        self,
        source_path: str,
        destination_path: str,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Move a file or directory in DBFS.
        
        Args:
            source_path: Source DBFS path
            destination_path: Destination DBFS path
            overwrite: Whether to overwrite existing destination
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Moving DBFS path from {source_path} to {destination_path}")
        
        self.validate_required_args({
            "source_path": source_path,
            "destination_path": destination_path
        }, ["source_path", "destination_path"])
        
        # Ensure DBFS paths start with /
        if not source_path.startswith("/"):
            source_path = "/" + source_path
        if not destination_path.startswith("/"):
            destination_path = "/" + destination_path
        
        command_args = [
            "fs", "mv",
            f"dbfs:{source_path}",
            f"dbfs:{destination_path}"
        ]
        
        if overwrite:
            command_args.append("--overwrite")
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def get_file_info(self, dbfs_path: str) -> Dict[str, Any]:
        """
        Get information about a file or directory in DBFS.
        
        Args:
            dbfs_path: DBFS path to get info for
            
        Returns:
            Dictionary containing file/directory information
        """
        logger.info(f"Getting info for DBFS path: {dbfs_path}")
        
        self.validate_required_args({"dbfs_path": dbfs_path}, ["dbfs_path"])
        
        # Ensure DBFS path starts with /
        if not dbfs_path.startswith("/"):
            dbfs_path = "/" + dbfs_path
        
        # Get parent directory listing and find the specific item
        parent_path = os.path.dirname(dbfs_path) or "/"
        result = await self.list_files(parent_path)
        
        # Find the specific item in the listing
        item_name = os.path.basename(dbfs_path)
        if "files" in result:
            for file_info in result["files"]:
                if file_info.get("path", "").endswith(item_name):
                    return file_info
        
        # If not found, try to list the path directly (might be a directory)
        try:
            return await self.list_files(dbfs_path)
        except Exception:
            raise ValueError(f"DBFS path not found: {dbfs_path}")
    
    async def copy_file(
        self,
        source_path: str,
        destination_path: str,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Copy a file in DBFS.
        
        Args:
            source_path: Source DBFS path
            destination_path: Destination DBFS path
            overwrite: Whether to overwrite existing destination
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Copying DBFS file from {source_path} to {destination_path}")
        
        self.validate_required_args({
            "source_path": source_path,
            "destination_path": destination_path
        }, ["source_path", "destination_path"])
        
        # Ensure DBFS paths start with /
        if not source_path.startswith("/"):
            source_path = "/" + source_path
        if not destination_path.startswith("/"):
            destination_path = "/" + destination_path
        
        command_args = [
            "fs", "cp",
            f"dbfs:{source_path}",
            f"dbfs:{destination_path}"
        ]
        
        if overwrite:
            command_args.append("--overwrite")
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)


# Create a global instance for easy importing
dbfs_cli = DBFSCLI()
