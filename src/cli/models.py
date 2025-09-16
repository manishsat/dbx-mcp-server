"""
Databricks Models CLI operations.

This module provides functions for managing ML models and Unity Catalog models using the CLI.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from src.cli.base import DatabricksCLI

logger = logging.getLogger(__name__)


class ModelsCLI(DatabricksCLI):
    """Databricks Models CLI operations."""
    
    # Unity Catalog Models
    async def list_models(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        max_results: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        List registered models in Unity Catalog.
        
        Args:
            catalog: Catalog to list models from
            schema: Schema to list models from
            max_results: Maximum number of results to return
            
        Returns:
            Dictionary containing list of models
        """
        logger.info("Listing Unity Catalog models")
        
        command_args = ["unity-catalog", "models", "list"]
        
        if catalog:
            command_args.extend(["--catalog-name", catalog])
        
        if schema:
            command_args.extend(["--schema-name", schema])
        
        if max_results:
            command_args.extend(["--max-results", str(max_results)])
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def get_model(self, model_name: str) -> Dict[str, Any]:
        """
        Get details of a specific Unity Catalog model.
        
        Args:
            model_name: Full name of the model (catalog.schema.model)
            
        Returns:
            Dictionary containing model details
        """
        logger.info(f"Getting Unity Catalog model: {model_name}")
        
        self.validate_required_args({"model_name": model_name}, ["model_name"])
        
        command_args = [
            "unity-catalog", "models", "get",
            model_name,
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def create_model(
        self,
        model_name: str,
        catalog: str,
        schema: str,
        comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new Unity Catalog model.
        
        Args:
            model_name: Name of the model to create
            catalog: Catalog to create the model in
            schema: Schema to create the model in
            comment: Optional comment for the model
            
        Returns:
            Dictionary containing created model details
        """
        logger.info(f"Creating Unity Catalog model: {catalog}.{schema}.{model_name}")
        
        self.validate_required_args({
            "model_name": model_name,
            "catalog": catalog,
            "schema": schema
        }, ["model_name", "catalog", "schema"])
        
        full_model_name = f"{catalog}.{schema}.{model_name}"
        
        command_args = [
            "unity-catalog", "models", "create",
            full_model_name
        ]
        
        if comment:
            command_args.extend(["--comment", comment])
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def delete_model(self, model_name: str) -> Dict[str, Any]:
        """
        Delete a Unity Catalog model.
        
        Args:
            model_name: Full name of the model to delete (catalog.schema.model)
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Deleting Unity Catalog model: {model_name}")
        
        self.validate_required_args({"model_name": model_name}, ["model_name"])
        
        command_args = [
            "unity-catalog", "models", "delete",
            model_name,
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    # Model Versions
    async def list_model_versions(
        self,
        model_name: str,
        max_results: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        List versions of a Unity Catalog model.
        
        Args:
            model_name: Full name of the model (catalog.schema.model)
            max_results: Maximum number of results to return
            
        Returns:
            Dictionary containing list of model versions
        """
        logger.info(f"Listing versions for model: {model_name}")
        
        self.validate_required_args({"model_name": model_name}, ["model_name"])
        
        command_args = [
            "unity-catalog", "model-versions", "list",
            model_name
        ]
        
        if max_results:
            command_args.extend(["--max-results", str(max_results)])
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def get_model_version(self, model_name: str, version: int) -> Dict[str, Any]:
        """
        Get details of a specific model version.
        
        Args:
            model_name: Full name of the model (catalog.schema.model)
            version: Version number
            
        Returns:
            Dictionary containing model version details
        """
        logger.info(f"Getting model version: {model_name} v{version}")
        
        self.validate_required_args({
            "model_name": model_name,
            "version": version
        }, ["model_name", "version"])
        
        command_args = [
            "unity-catalog", "model-versions", "get",
            model_name,
            str(version),
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def create_model_version(
        self,
        model_name: str,
        source: str,
        run_id: Optional[str] = None,
        comment: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new model version.
        
        Args:
            model_name: Full name of the model (catalog.schema.model)
            source: Source path for the model artifacts
            run_id: MLflow run ID associated with this version
            comment: Optional comment for the version
            
        Returns:
            Dictionary containing created model version details
        """
        logger.info(f"Creating model version for: {model_name}")
        
        self.validate_required_args({
            "model_name": model_name,
            "source": source
        }, ["model_name", "source"])
        
        command_args = [
            "unity-catalog", "model-versions", "create",
            model_name,
            "--source", source
        ]
        
        if run_id:
            command_args.extend(["--run-id", run_id])
        
        if comment:
            command_args.extend(["--comment", comment])
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def delete_model_version(
        self,
        model_name: str,
        version: int
    ) -> Dict[str, Any]:
        """
        Delete a model version.
        
        Args:
            model_name: Full name of the model (catalog.schema.model)
            version: Version number to delete
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Deleting model version: {model_name} v{version}")
        
        self.validate_required_args({
            "model_name": model_name,
            "version": version
        }, ["model_name", "version"])
        
        command_args = [
            "unity-catalog", "model-versions", "delete",
            model_name,
            str(version),
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    # Model Aliases (for Unity Catalog)
    async def set_model_alias(
        self,
        model_name: str,
        alias: str,
        version: int
    ) -> Dict[str, Any]:
        """
        Set an alias for a model version.
        
        Args:
            model_name: Full name of the model (catalog.schema.model)
            alias: Alias name (e.g., "champion", "challenger")
            version: Version number to assign the alias to
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Setting alias {alias} for {model_name} v{version}")
        
        self.validate_required_args({
            "model_name": model_name,
            "alias": alias,
            "version": version
        }, ["model_name", "alias", "version"])
        
        command_args = [
            "unity-catalog", "model-versions", "set-alias",
            model_name,
            alias,
            str(version),
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def delete_model_alias(
        self,
        model_name: str,
        alias: str
    ) -> Dict[str, Any]:
        """
        Delete a model alias.
        
        Args:
            model_name: Full name of the model (catalog.schema.model)
            alias: Alias name to delete
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Deleting alias {alias} from {model_name}")
        
        self.validate_required_args({
            "model_name": model_name,
            "alias": alias
        }, ["model_name", "alias"])
        
        command_args = [
            "unity-catalog", "model-versions", "delete-alias",
            model_name,
            alias,
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    # MLflow Models (for workspace model registry)
    async def list_mlflow_models(self, max_results: Optional[int] = None) -> Dict[str, Any]:
        """
        List MLflow registered models in workspace model registry.
        
        Args:
            max_results: Maximum number of results to return
            
        Returns:
            Dictionary containing list of MLflow models
        """
        logger.info("Listing MLflow models")
        
        command_args = ["model-registry", "list-models"]
        
        if max_results:
            command_args.extend(["--max-results", str(max_results)])
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def get_mlflow_model(self, model_name: str) -> Dict[str, Any]:
        """
        Get details of a specific MLflow model.
        
        Args:
            model_name: Name of the MLflow model
            
        Returns:
            Dictionary containing MLflow model details
        """
        logger.info(f"Getting MLflow model: {model_name}")
        
        self.validate_required_args({"model_name": model_name}, ["model_name"])
        
        command_args = [
            "model-registry", "get-model",
            model_name,
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def create_mlflow_model(
        self,
        model_name: str,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new MLflow model in workspace model registry.
        
        Args:
            model_name: Name of the model to create
            description: Optional description for the model
            
        Returns:
            Dictionary containing created MLflow model details
        """
        logger.info(f"Creating MLflow model: {model_name}")
        
        self.validate_required_args({"model_name": model_name}, ["model_name"])
        
        command_args = [
            "model-registry", "create-model",
            model_name
        ]
        
        if description:
            command_args.extend(["--description", description])
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def get_latest_model_versions(
        self,
        model_name: str,
        stages: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Get latest versions of an MLflow model by stage.
        
        Args:
            model_name: Name of the MLflow model
            stages: List of stages to get versions for (e.g., ["Production", "Staging"])
            
        Returns:
            Dictionary containing latest model versions
        """
        logger.info(f"Getting latest versions for MLflow model: {model_name}")
        
        self.validate_required_args({"model_name": model_name}, ["model_name"])
        
        command_args = [
            "model-registry", "get-latest-versions",
            model_name
        ]
        
        if stages:
            command_args.extend(["--stages", ",".join(stages)])
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)
    
    async def transition_model_stage(
        self,
        model_name: str,
        version: int,
        stage: str,
        archive_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Transition an MLflow model version to a different stage.
        
        Args:
            model_name: Name of the MLflow model
            version: Version number
            stage: Target stage (e.g., "Production", "Staging", "Archived")
            archive_existing: Whether to archive existing model in target stage
            
        Returns:
            Dictionary containing transition result
        """
        logger.info(f"Transitioning {model_name} v{version} to {stage}")
        
        self.validate_required_args({
            "model_name": model_name,
            "version": version,
            "stage": stage
        }, ["model_name", "version", "stage"])
        
        command_args = [
            "model-registry", "transition-stage",
            model_name,
            str(version),
            "--stage", stage
        ]
        
        if archive_existing:
            command_args.append("--archive-existing-versions")
        
        command_args.extend(["--output", "json"])
        
        return await self.execute(command_args)


# Create a global instance for easy importing
models_cli = ModelsCLI()
