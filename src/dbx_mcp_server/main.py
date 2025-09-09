"""Main entry point for the Databricks MCP Server."""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence

import click
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListToolsRequest,
    ListToolsResult,
    Tool,
    TextContent,
)

from .databricks_client import DatabricksClient


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabricksMCPServer:
    """Databricks MCP Server implementation."""
    
    def __init__(self) -> None:
        self.server = Server("databricks-mcp-server")
        self.databricks_client = DatabricksClient()
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Set up MCP request handlers."""
        
        @self.server.list_tools()
        async def handle_list_tools() -> ListToolsResult:
            """List available Databricks tools."""
            return ListToolsResult(
                tools=[
                    Tool(
                        name="create_job",
                        description="Create a new Databricks job",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Name of the job"
                                },
                                "notebook_path": {
                                    "type": "string", 
                                    "description": "Path to the notebook in Databricks workspace"
                                },
                                "cluster_id": {
                                    "type": "string",
                                    "description": "ID of the cluster to run the job on"
                                },
                                "parameters": {
                                    "type": "object",
                                    "description": "Job parameters as key-value pairs",
                                    "additionalProperties": {"type": "string"}
                                }
                            },
                            "required": ["name", "notebook_path"]
                        }
                    ),
                    Tool(
                        name="list_jobs",
                        description="List all jobs in the Databricks workspace",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "limit": {
                                    "type": "integer",
                                    "description": "Maximum number of jobs to return",
                                    "default": 25
                                }
                            }
                        }
                    ),
                    Tool(
                        name="get_job",
                        description="Get details of a specific job",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "job_id": {
                                    "type": "integer",
                                    "description": "ID of the job to retrieve"
                                }
                            },
                            "required": ["job_id"]
                        }
                    ),
                    Tool(
                        name="run_job",
                        description="Trigger a job run",
                        inputSchema={
                            "type": "object",
                            "properties": {
                                "job_id": {
                                    "type": "integer",
                                    "description": "ID of the job to run"
                                },
                                "parameters": {
                                    "type": "object",
                                    "description": "Runtime parameters for the job",
                                    "additionalProperties": {"type": "string"}
                                }
                            },
                            "required": ["job_id"]
                        }
                    ),
                    Tool(
                        name="list_clusters",
                        description="List available clusters in the workspace",
                        inputSchema={
                            "type": "object",
                            "properties": {}
                        }
                    )
                ]
            )

        @self.server.call_tool()
        async def handle_call_tool(request: CallToolRequest) -> CallToolResult:
            """Handle tool calls."""
            try:
                if request.name == "create_job":
                    result = await self._create_job(request.arguments or {})
                elif request.name == "list_jobs":
                    result = await self._list_jobs(request.arguments or {})
                elif request.name == "get_job":
                    result = await self._get_job(request.arguments or {})
                elif request.name == "run_job":
                    result = await self._run_job(request.arguments or {})
                elif request.name == "list_clusters":
                    result = await self._list_clusters(request.arguments or {})
                else:
                    raise ValueError(f"Unknown tool: {request.name}")
                
                return CallToolResult(
                    content=[TextContent(type="text", text=str(result))]
                )
            except Exception as e:
                logger.error(f"Error calling tool {request.name}: {e}")
                return CallToolResult(
                    content=[TextContent(type="text", text=f"Error: {e}")],
                    isError=True
                )

    async def _create_job(self, args: Dict[str, Any]) -> str:
        """Create a new Databricks job."""
        name = args.get("name")
        notebook_path = args.get("notebook_path")
        cluster_id = args.get("cluster_id")
        parameters = args.get("parameters", {})
        
        job_id = self.databricks_client.create_job(
            name=name,
            notebook_path=notebook_path,
            cluster_id=cluster_id,
            parameters=parameters
        )
        
        return f"Successfully created job '{name}' with ID: {job_id}"

    async def _list_jobs(self, args: Dict[str, Any]) -> str:
        """List all jobs in the workspace."""
        limit = args.get("limit", 25)
        jobs = self.databricks_client.list_jobs(limit=limit)
        
        if not jobs:
            return "No jobs found in the workspace"
        
        result = "Jobs in workspace:\n"
        for job in jobs:
            result += f"- {job['job_id']}: {job['settings']['name']}\n"
        
        return result

    async def _get_job(self, args: Dict[str, Any]) -> str:
        """Get details of a specific job."""
        job_id = args.get("job_id")
        job = self.databricks_client.get_job(job_id=job_id)
        
        settings = job.get("settings", {})
        return f"Job {job_id} details:\n" \
               f"Name: {settings.get('name', 'N/A')}\n" \
               f"Created by: {job.get('creator_user_name', 'N/A')}\n" \
               f"Created at: {job.get('created_time', 'N/A')}\n"

    async def _run_job(self, args: Dict[str, Any]) -> str:
        """Trigger a job run."""
        job_id = args.get("job_id")
        parameters = args.get("parameters", {})
        
        run_id = self.databricks_client.run_job(
            job_id=job_id, 
            parameters=parameters
        )
        
        return f"Successfully triggered job {job_id}. Run ID: {run_id}"

    async def _list_clusters(self, args: Dict[str, Any]) -> str:
        """List available clusters."""
        clusters = self.databricks_client.list_clusters()
        
        if not clusters:
            return "No clusters found in the workspace"
        
        result = "Available clusters:\n"
        for cluster in clusters:
            result += f"- {cluster['cluster_id']}: {cluster['cluster_name']} "
            result += f"({cluster['state']})\n"
        
        return result

    async def run(self) -> None:
        """Run the MCP server."""
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="databricks-mcp-server",
                    server_version="0.1.0"
                )
            )


@click.command()
@click.option("--debug", is_flag=True, help="Enable debug logging")
def main(debug: bool) -> None:
    """Run the Databricks MCP Server."""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    server = DatabricksMCPServer()
    asyncio.run(server.run())


if __name__ == "__main__":
    main()