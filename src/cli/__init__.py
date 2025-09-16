"""
Databricks CLI operations package.

This package contains CLI wrappers for various Databricks operations.
"""

from src.cli.base import DatabricksCLI
from src.cli.clusters import ClustersCLI, clusters_cli
from src.cli.dbfs import DBFSCLI, dbfs_cli
from src.cli.jobs import JobsCLI, jobs_cli
from src.cli.models import ModelsCLI, models_cli
from src.cli.notebooks import NotebooksCLI, notebooks_cli
from src.cli.sql import SQLCLI, sql_cli

__all__ = [
    # Base class
    "DatabricksCLI",
    
    # CLI classes
    "ClustersCLI",
    "DBFSCLI", 
    "JobsCLI",
    "ModelsCLI",
    "NotebooksCLI",
    "SQLCLI",
    
    # Global instances
    "clusters_cli",
    "dbfs_cli",
    "jobs_cli", 
    "models_cli",
    "notebooks_cli",
    "sql_cli",
]
