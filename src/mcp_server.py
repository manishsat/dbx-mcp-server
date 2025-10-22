"""
Databricks CLI MCP Server

Production-ready MCP server providing 33 comprehensive tools for Databricks automation
across clusters (including library management), jobs (including debugging), workspace, and file system operations.
"""

import asyncio
import json

from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    TextContent,
    Tool,
)

from src.cli.clusters import clusters_cli
from src.cli.dbfs import dbfs_cli
from src.cli.jobs import jobs_cli
from src.cli.workspace import workspace_cli
from src.core.config import settings
from src.core.utils import setup_logging

# Setup logging
logger = setup_logging()

# MCP Server instance
server = Server("databricks-mcp")


@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    """List available tools - clusters and jobs operations."""
    return [
        # Cluster operations
        Tool(
            name="list_clusters",
            description="List all clusters in the Databricks workspace",
            inputSchema={
                "type": "object",
                "properties": {},
                "additionalProperties": False
            }
        ),
        Tool(
            name="get_cluster",
            description="Get details of a specific cluster by ID",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "The ID of the cluster to get details for"
                    }
                },
                "required": ["cluster_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="create_cluster",
            description="Create a new cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "Name of the cluster to create"
                    },
                    "spark_version": {
                        "type": "string",
                        "description": "Spark version for the cluster"
                    },
                    "node_type_id": {
                        "type": "string", 
                        "description": "Node type ID for the cluster"
                    },
                    "driver_node_type_id": {
                        "type": "string",
                        "description": "Driver node type ID (optional)"
                    },
                    "num_workers": {
                        "type": "integer",
                        "description": "Number of worker nodes (optional)"
                    },
                    "autoscale_min_workers": {
                        "type": "integer",
                        "description": "Minimum workers for autoscaling (optional)"
                    },
                    "autoscale_max_workers": {
                        "type": "integer", 
                        "description": "Maximum workers for autoscaling (optional)"
                    },
                    "cluster_config_json": {
                        "type": "string",
                        "description": "JSON string with complete cluster configuration (optional)"
                    }
                },
                "required": ["cluster_name", "spark_version", "node_type_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="start_cluster",
            description="Start a stopped cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "The ID of the cluster to start"
                    }
                },
                "required": ["cluster_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="terminate_cluster",
            description="Terminate a running cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "The ID of the cluster to terminate"
                    }
                },
                "required": ["cluster_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="delete_cluster",
            description="Permanently delete a cluster",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "The ID of the cluster to delete permanently"
                    }
                },
                "required": ["cluster_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="find_cluster_by_name",
            description="Find a cluster by name with optional state filtering",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "The name of the cluster to find"
                    },
                    "state": {
                        "type": "string",
                        "description": "Optional state filter (e.g., 'RUNNING', 'TERMINATED'). Defaults to preferring RUNNING clusters.",
                        "enum": ["RUNNING", "TERMINATED", "PENDING", "RESTARTING", "TERMINATING", "ERROR", "UNKNOWN"]
                    }
                },
                "required": ["cluster_name"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="install_libraries",
            description="Install libraries on a Databricks cluster (Maven, PyPI, or workspace wheel files)",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "The ID of the cluster to install libraries on"
                    },
                    "libraries": {
                        "type": "array",
                        "description": "List of libraries to install",
                        "items": {
                            "type": "object",
                            "description": "Library specification",
                            "anyOf": [
                                {
                                    "properties": {
                                        "maven": {
                                            "type": "object",
                                            "properties": {
                                                "coordinates": {
                                                    "type": "string",
                                                    "description": "Maven coordinates (e.g., 'org.jsoup:jsoup:1.7.2')"
                                                },
                                                "repo": {
                                                    "type": "string", 
                                                    "description": "Optional Maven repository URL"
                                                },
                                                "exclusions": {
                                                    "type": "array",
                                                    "items": {"type": "string"},
                                                    "description": "Optional Maven exclusions"
                                                }
                                            },
                                            "required": ["coordinates"]
                                        }
                                    },
                                    "required": ["maven"]
                                },
                                {
                                    "properties": {
                                        "pypi": {
                                            "type": "object",
                                            "properties": {
                                                "package": {
                                                    "type": "string",
                                                    "description": "PyPI package name (e.g., 'pandas==1.3.0' or 'numpy')"
                                                },
                                                "repo": {
                                                    "type": "string",
                                                    "description": "Optional PyPI repository URL"
                                                }
                                            },
                                            "required": ["package"]
                                        }
                                    },
                                    "required": ["pypi"]
                                },
                                {
                                    "properties": {
                                        "whl": {
                                            "type": "string",
                                            "description": "Path to wheel file in workspace (e.g., '/Workspace/path/to/library.whl')"
                                        }
                                    },
                                    "required": ["whl"]
                                }
                            ]
                        }
                    }
                },
                "required": ["cluster_id", "libraries"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="uninstall_libraries",
            description="Uninstall libraries from a Databricks cluster (requires cluster restart)",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "The ID of the cluster to uninstall libraries from"
                    },
                    "libraries": {
                        "type": "array",
                        "description": "List of libraries to uninstall (same format as install_libraries)",
                        "items": {
                            "type": "object",
                            "description": "Library specification to uninstall"
                        }
                    }
                },
                "required": ["cluster_id", "libraries"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="list_cluster_libraries",
            description="List all libraries installed on a Databricks cluster with their status",
            inputSchema={
                "type": "object",
                "properties": {
                    "cluster_id": {
                        "type": "string",
                        "description": "The ID of the cluster to query libraries for"
                    }
                },
                "required": ["cluster_id"],
                "additionalProperties": False
            }
        ),
        
        # Workspace operations
        Tool(
            name="list_workspace",
            description="List items in workspace directory",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace path to list (default: /)",
                        "default": "/"
                    },
                    "recursive": {
                        "type": "boolean",
                        "description": "List items recursively (default: false)",
                        "default": False
                    }
                },
                "additionalProperties": False
            }
        ),
        Tool(
            name="get_workspace_item", 
            description="Get details of a specific workspace item",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace path of the item to get details for"
                    }
                },
                "required": ["path"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="create_notebook",
            description="Create a notebook in the workspace with given content", 
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace path where notebook should be created"
                    },
                    "content": {
                        "type": "string",
                        "description": "Content of the notebook (Python code)"
                    },
                    "language": {
                        "type": "string",
                        "description": "Notebook language (PYTHON, SCALA, SQL, R)",
                        "default": "PYTHON"
                    }
                },
                "required": ["path", "content"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="upload_notebook",
            description="Upload a local notebook file to workspace",
            inputSchema={
                "type": "object", 
                "properties": {
                    "local_path": {
                        "type": "string",
                        "description": "Local file path of notebook to upload"
                    },
                    "workspace_path": {
                        "type": "string", 
                        "description": "Workspace path where notebook should be uploaded"
                    },
                    "language": {
                        "type": "string",
                        "description": "Notebook language (PYTHON, SCALA, SQL, R)",
                        "default": "PYTHON"
                    }
                },
                "required": ["local_path", "workspace_path"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="delete_workspace_item",
            description="Delete a workspace item (notebook, folder, etc.)",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string", 
                        "description": "Workspace path of item to delete"
                    },
                    "recursive": {
                        "type": "boolean",
                        "description": "Delete recursively if it's a folder (default: false)",
                        "default": False
                    }
                },
                "required": ["path"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="create_workspace_directory",
            description="Create a directory in the workspace",
            inputSchema={
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace path of directory to create"
                    }
                },
                "required": ["path"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="get_user_workspace_path", 
            description="Get current user's workspace path for notebook uploads",
            inputSchema={
                "type": "object",
                "properties": {},
                "additionalProperties": False
            }
        ),
        Tool(
            name="setup_user_workspace",
            description="Create user workspace directory structure for MCP operations",
            inputSchema={
                "type": "object",
                "properties": {
                    "subdirs": {
                        "type": "array",
                        "description": "Optional subdirectories to create (e.g. ['notebooks', 'scripts'])",
                        "items": {
                            "type": "string",
                            "description": "Directory name to create"
                        }
                    }
                },
                "additionalProperties": False
            }
        ),
        
        # Job operations
        Tool(
            name="list_jobs",
            description="List Databricks jobs in the workspace (defaults to current user's jobs)",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of jobs to return (default: 25)",
                        "minimum": 1,
                        "maximum": 100
                    },
                    "created_by": {
                        "type": "string",
                        "description": "Filter by creator username (default: current user). Use 'all' to list all jobs in workspace"
                    },
                    "include_all_users": {
                        "type": "boolean",
                        "description": "If true, includes jobs from all users (overrides created_by filter). Default: false"
                    }
                },
                "additionalProperties": False
            }
        ),
        Tool(
            name="get_job",
            description="Get detailed information about a specific job",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The ID of the job to retrieve"
                    }
                },
                "required": ["job_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="create_job",
            description="Create a new Databricks job with configuration. Can use existing cluster or create new one.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_config": {
                        "type": "object",
                        "description": "Complete job configuration as JSON object",
                        "properties": {
                            "name": {
                                "type": "string",
                                "description": "Job name (required)"
                            },
                            "tasks": {
                                "type": "array",
                                "description": "Job tasks array. Can specify existing_cluster_id OR new_cluster for each task",
                                "items": {
                                    "type": "object",
                                    "description": "Individual job task configuration"
                                }
                            }
                        },
                        "required": ["name"]
                    },
                    "existing_cluster_name": {
                        "type": "string",
                        "description": "Optional: Name of existing cluster to use. If provided, will override new_cluster settings in tasks."
                    },
                    "existing_cluster_id": {
                        "type": "string", 
                        "description": "Optional: ID of existing cluster to use. Takes precedence over cluster_name."
                    }
                },
                "required": ["job_config"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="update_job",
            description="Update an existing job's configuration. Use this to modify job settings like schedule, pause/unpause jobs, update tasks, etc. The job_id should NOT be included in the job_config - provide it as a separate parameter.",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The ID of the job to update (provide as separate parameter, not in job_config)"
                    },
                    "job_config": {
                        "type": "object",
                        "description": "Updated job configuration as JSON object. Do NOT include job_id in this object - it will be added automatically. Examples: {\"schedule\": {\"pause_status\": \"UNPAUSED\"}}, {\"max_concurrent_runs\": 2}, {\"tasks\": [...]}"
                    }
                },
                "required": ["job_id", "job_config"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="delete_job",
            description="Permanently delete a job",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The ID of the job to delete"
                    }
                },
                "required": ["job_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="run_job",
            description="Trigger a job run with optional parameters",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The ID of the job to run"
                    },
                    "parameters": {
                        "type": "object",
                        "description": "Optional parameters for the job run"
                    },
                    "idempotency_token": {
                        "type": "string",
                        "description": "Optional token to guarantee idempotency"
                    }
                },
                "required": ["job_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="cancel_job_run",
            description="Cancel a running job execution",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The ID of the job run to cancel"
                    }
                },
                "required": ["run_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="get_job_run",
            description="Get details and status of a job run, including task structure for multi-task jobs. Use this first to identify individual task run IDs for debugging failed tasks.",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The job run ID (not task run ID) to retrieve details for"
                    }
                },
                "required": ["run_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="get_job_run_output",
            description="Get output, logs, and error details from a job run for debugging. For multi-task jobs, use individual task run IDs (found in get_job_run response) rather than the job run ID.",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The ID of the job run to get output and logs for"
                    }
                },
                "required": ["run_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="export_job_run",
            description="Export comprehensive job run information including code, configuration, and metadata. For multi-task jobs that failed, first use get_job_run to identify failed tasks, then use this tool with task run IDs for detailed debugging.",
            inputSchema={
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "The ID of the job run to export"
                    },
                    "views_to_export": {
                        "type": "string",
                        "description": "Which views to export (CODE, DASHBOARDS, or ALL) - default: ALL",
                        "enum": ["CODE", "DASHBOARDS", "ALL"],
                        "default": "ALL"
                    }
                },
                "required": ["run_id"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="list_job_runs",
            description="List job run history with optional filtering",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "Optional job ID to filter runs for specific job"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of runs to return (default: 25)",
                        "minimum": 1,
                        "maximum": 100
                    }
                },
                "additionalProperties": False
            }
        ),
        
        # DBFS operations
        Tool(
            name="list_files",
            description="List files and directories in a DBFS path",
            inputSchema={
                "type": "object",
                "properties": {
                    "dbfs_path": {
                        "type": "string",
                        "description": "DBFS path to list (default: root /)",
                        "default": "/"
                    }
                },
                "additionalProperties": False
            }
        ),
        Tool(
            name="upload_file",
            description="Upload a local file to DBFS",
            inputSchema={
                "type": "object",
                "properties": {
                    "local_path": {
                        "type": "string",
                        "description": "Local path to the file to upload"
                    },
                    "dbfs_path": {
                        "type": "string",
                        "description": "Target DBFS path where to upload the file"
                    },
                    "overwrite": {
                        "type": "boolean",
                        "description": "Whether to overwrite existing file (default: false)",
                        "default": False
                    }
                },
                "required": ["local_path", "dbfs_path"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="download_file",
            description="Download a file from DBFS to local filesystem",
            inputSchema={
                "type": "object",
                "properties": {
                    "dbfs_path": {
                        "type": "string",
                        "description": "DBFS path to the file to download"
                    },
                    "local_path": {
                        "type": "string",
                        "description": "Local path where to save the downloaded file"
                    },
                    "overwrite": {
                        "type": "boolean",
                        "description": "Whether to overwrite existing local file (default: false)",
                        "default": False
                    }
                },
                "required": ["dbfs_path", "local_path"],
                "additionalProperties": False
            }
        ),
        Tool(
            name="delete_file",
            description="Delete a file or directory from DBFS",
            inputSchema={
                "type": "object",
                "properties": {
                    "dbfs_path": {
                        "type": "string",
                        "description": "DBFS path to delete"
                    },
                    "recursive": {
                        "type": "boolean",
                        "description": "Whether to delete recursively (for directories, default: false)",
                        "default": False
                    }
                },
                "required": ["dbfs_path"],
                "additionalProperties": False
            }
        )
    ]


@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Handle tool calls - clusters and jobs operations."""
    
    logger.info(f"Handling tool call: {name} with arguments: {arguments}")
    
    try:
        # Cluster operations
        if name == "list_clusters":
            result = await clusters_cli.list_clusters()
            
        elif name == "get_cluster":
            cluster_id = arguments.get("cluster_id")
            if not cluster_id:
                raise ValueError("cluster_id is required")
            result = await clusters_cli.get_cluster(cluster_id)
            
        elif name == "create_cluster":
            # Extract required arguments
            cluster_name = arguments.get("cluster_name")
            spark_version = arguments.get("spark_version") 
            node_type_id = arguments.get("node_type_id")
            
            if not all([cluster_name, spark_version, node_type_id]):
                raise ValueError("cluster_name, spark_version, and node_type_id are required")
            
            # Build cluster config
            cluster_config = {
                "cluster_name": cluster_name,
                "spark_version": spark_version,
                "node_type_id": node_type_id
            }
            
            # Add optional parameters
            if arguments.get("driver_node_type_id"):
                cluster_config["driver_node_type_id"] = arguments["driver_node_type_id"]
            
            if arguments.get("num_workers") is not None:
                cluster_config["num_workers"] = arguments["num_workers"]
            
            if arguments.get("autoscale_min_workers") is not None and arguments.get("autoscale_max_workers") is not None:
                cluster_config["autoscale"] = {
                    "min_workers": arguments["autoscale_min_workers"],
                    "max_workers": arguments["autoscale_max_workers"]
                }
            
            # Handle complete JSON config if provided
            if arguments.get("cluster_config_json"):
                try:
                    json_config = json.loads(arguments["cluster_config_json"])
                    cluster_config.update(json_config)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON in cluster_config_json: {e}")
            
            result = await clusters_cli.create_cluster(cluster_config)
            
        elif name == "start_cluster":
            cluster_id = arguments.get("cluster_id")
            if not cluster_id:
                raise ValueError("cluster_id is required")
            result = await clusters_cli.start_cluster(cluster_id)
            
        elif name == "terminate_cluster":
            cluster_id = arguments.get("cluster_id")
            if not cluster_id:
                raise ValueError("cluster_id is required") 
            result = await clusters_cli.terminate_cluster(cluster_id)
            
        elif name == "delete_cluster":
            cluster_id = arguments.get("cluster_id")
            if not cluster_id:
                raise ValueError("cluster_id is required")
            result = await clusters_cli.delete_cluster(cluster_id)
            
        elif name == "find_cluster_by_name":
            cluster_name = arguments.get("cluster_name")
            if not cluster_name:
                raise ValueError("cluster_name is required")
            state = arguments.get("state")  # Optional state filter
            result = await clusters_cli.find_cluster_by_name(cluster_name, state)
            
        elif name == "install_libraries":
            cluster_id = arguments.get("cluster_id")
            libraries = arguments.get("libraries")
            if not cluster_id:
                raise ValueError("cluster_id is required")
            if not libraries:
                raise ValueError("libraries is required")
            result = await clusters_cli.install_libraries(cluster_id, libraries)
            
        elif name == "uninstall_libraries":
            cluster_id = arguments.get("cluster_id")
            libraries = arguments.get("libraries")
            if not cluster_id:
                raise ValueError("cluster_id is required")
            if not libraries:
                raise ValueError("libraries is required")
            result = await clusters_cli.uninstall_libraries(cluster_id, libraries)
            
        elif name == "list_cluster_libraries":
            cluster_id = arguments.get("cluster_id")
            if not cluster_id:
                raise ValueError("cluster_id is required")
            result = await clusters_cli.list_cluster_libraries(cluster_id)
            
        # Workspace operations
        elif name == "list_workspace":
            path = arguments.get("path", "/")
            recursive = arguments.get("recursive", False)
            result = await workspace_cli.list_workspace_items(path, recursive)
            
        elif name == "get_workspace_item":
            path = arguments.get("path")
            if not path:
                raise ValueError("path is required")
            result = await workspace_cli.get_workspace_item(path)
            
        elif name == "create_notebook":
            path = arguments.get("path")
            content = arguments.get("content")
            language = arguments.get("language", "PYTHON")
            if not path:
                raise ValueError("path is required")
            if not content:
                raise ValueError("content is required")
            result = await workspace_cli.create_notebook(path, content, language)
            
        elif name == "upload_notebook":
            local_path = arguments.get("local_path")
            workspace_path = arguments.get("workspace_path")
            language = arguments.get("language", "PYTHON")
            if not local_path:
                raise ValueError("local_path is required")
            if not workspace_path:
                raise ValueError("workspace_path is required")
            result = await workspace_cli.upload_notebook(local_path, workspace_path, language)
            
        elif name == "delete_workspace_item":
            path = arguments.get("path")
            recursive = arguments.get("recursive", False)
            if not path:
                raise ValueError("path is required")
            result = await workspace_cli.delete_workspace_item(path, recursive)
            
        elif name == "create_workspace_directory":
            path = arguments.get("path")
            if not path:
                raise ValueError("path is required")
            result = await workspace_cli.create_directory(path)
            
        elif name == "get_user_workspace_path":
            result = {"user_workspace_path": await workspace_cli.get_user_workspace_path()}
            
        elif name == "setup_user_workspace":
            subdirs = arguments.get("subdirs", ["notebooks", "scripts", "temp"])
            result = await workspace_cli.create_user_directory_structure(subdirs)
            
        # Jobs operations
        elif name == "list_jobs":
            limit = arguments.get("limit", 25)
            created_by = arguments.get("created_by")
            include_all_users = arguments.get("include_all_users", False)
            result = await jobs_cli.list_jobs(limit, created_by, include_all_users)
            
        elif name == "get_job":
            job_id = arguments.get("job_id")
            if not job_id:
                raise ValueError("job_id is required")
            result = await jobs_cli.get_job(job_id)
            
        elif name == "create_job":
            job_config = arguments.get("job_config")
            if not job_config:
                raise ValueError("job_config is required")
            existing_cluster_name = arguments.get("existing_cluster_name")
            existing_cluster_id = arguments.get("existing_cluster_id")
            result = await jobs_cli.create_job(job_config, existing_cluster_name, existing_cluster_id)
            
        elif name == "update_job":
            job_id = arguments.get("job_id")
            job_config = arguments.get("job_config")
            if not job_id:
                raise ValueError("job_id is required")
            if not job_config:
                raise ValueError("job_config is required")
            result = await jobs_cli.update_job(job_id, job_config)
            
        elif name == "delete_job":
            job_id = arguments.get("job_id")
            if not job_id:
                raise ValueError("job_id is required")
            result = await jobs_cli.delete_job(job_id)
            
        elif name == "run_job":
            job_id = arguments.get("job_id")
            if not job_id:
                raise ValueError("job_id is required")
            parameters = arguments.get("parameters")
            idempotency_token = arguments.get("idempotency_token")
            result = await jobs_cli.run_job(job_id, parameters, idempotency_token)
            
        elif name == "cancel_job_run":
            run_id = arguments.get("run_id")
            if not run_id:
                raise ValueError("run_id is required")
            result = await jobs_cli.cancel_job_run(run_id)
            
        elif name == "get_job_run":
            run_id = arguments.get("run_id")
            if not run_id:
                raise ValueError("run_id is required")
            result = await jobs_cli.get_job_run(run_id)
            
        elif name == "get_job_run_output":
            run_id = arguments.get("run_id")
            if not run_id:
                raise ValueError("run_id is required")
            result = await jobs_cli.get_job_run_output(run_id)
            
        elif name == "export_job_run":
            run_id = arguments.get("run_id")
            views_to_export = arguments.get("views_to_export", "ALL")
            if not run_id:
                raise ValueError("run_id is required")
            result = await jobs_cli.export_job_run(run_id, views_to_export)
            
        elif name == "list_job_runs":
            job_id = arguments.get("job_id")  # Optional
            limit = arguments.get("limit", 25)
            result = await jobs_cli.list_job_runs(job_id, limit)
            
        # DBFS operations
        elif name == "list_files":
            dbfs_path = arguments.get("dbfs_path", "/")
            result = await dbfs_cli.list_files(dbfs_path)
            
        elif name == "upload_file":
            local_path = arguments.get("local_path")
            dbfs_path = arguments.get("dbfs_path")
            overwrite = arguments.get("overwrite", False)
            if not local_path:
                raise ValueError("local_path is required")
            if not dbfs_path:
                raise ValueError("dbfs_path is required")
            result = await dbfs_cli.upload_file(local_path, dbfs_path, overwrite)
            
        elif name == "download_file":
            dbfs_path = arguments.get("dbfs_path")
            local_path = arguments.get("local_path")
            overwrite = arguments.get("overwrite", False)
            if not dbfs_path:
                raise ValueError("dbfs_path is required")
            if not local_path:
                raise ValueError("local_path is required")
            result = await dbfs_cli.download_file(dbfs_path, local_path, overwrite)
            
        elif name == "delete_file":
            dbfs_path = arguments.get("dbfs_path")
            recursive = arguments.get("recursive", False)
            if not dbfs_path:
                raise ValueError("dbfs_path is required")
            result = await dbfs_cli.delete_file(dbfs_path, recursive)
            
        else:
            raise ValueError(f"Unknown tool: {name}")
        
        # Format the result as JSON string for the MCP client
        result_text = json.dumps(result, indent=2, default=str)
        return [TextContent(type="text", text=result_text)]
        
    except Exception as e:
        logger.error(f"Error handling tool call {name}: {str(e)}", exc_info=True)
        error_result = {
            "error": str(e),
            "tool": name,
            "arguments": arguments
        }
        return [TextContent(type="text", text=json.dumps(error_result, indent=2))]


async def main():
    """Main server function."""
    logger.info("Starting Databricks MCP Server")
    logger.info(f"Server: {settings.mcp_server_name} v{settings.mcp_server_version}")
    logger.info(f"Available tools: 33 (clusters + libraries, jobs + debugging, workspace, DBFS)")
    logger.info(f"Databricks profile: {settings.databricks_profile or 'default'}")
    
    # Run the server using stdio transport
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name=settings.mcp_server_name,
                server_version=settings.mcp_server_version,
                capabilities={
                    "tools": {}
                },
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
