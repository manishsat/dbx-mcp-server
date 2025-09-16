"""
Databricks SQL CLI operations.

This module provides functions for managing SQL queries, warehouses, and data sources using the CLI.
"""

import logging
from typing import Any, Dict, List, Optional

from src.cli.base import DatabricksCLI

logger = logging.getLogger(__name__)


class SQLCLI(DatabricksCLI):
    """Databricks SQL CLI operations."""
    
    async def list_warehouses(self) -> Dict[str, Any]:
        """
        List SQL warehouses.
        
        Returns:
            Dictionary containing list of warehouses
        """
        logger.info("Listing SQL warehouses")
        
        command_args = [
            "sql", "warehouses", "list",
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def get_warehouse(self, warehouse_id: str) -> Dict[str, Any]:
        """
        Get details of a specific SQL warehouse.
        
        Args:
            warehouse_id: ID of the warehouse to get
            
        Returns:
            Dictionary containing warehouse details
        """
        logger.info(f"Getting warehouse details: {warehouse_id}")
        
        self.validate_required_args({"warehouse_id": warehouse_id}, ["warehouse_id"])
        
        command_args = [
            "sql", "warehouses", "get",
            warehouse_id,
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def start_warehouse(self, warehouse_id: str) -> Dict[str, Any]:
        """
        Start a SQL warehouse.
        
        Args:
            warehouse_id: ID of the warehouse to start
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Starting warehouse: {warehouse_id}")
        
        self.validate_required_args({"warehouse_id": warehouse_id}, ["warehouse_id"])
        
        command_args = [
            "sql", "warehouses", "start",
            warehouse_id,
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def stop_warehouse(self, warehouse_id: str) -> Dict[str, Any]:
        """
        Stop a SQL warehouse.
        
        Args:
            warehouse_id: ID of the warehouse to stop
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Stopping warehouse: {warehouse_id}")
        
        self.validate_required_args({"warehouse_id": warehouse_id}, ["warehouse_id"])
        
        command_args = [
            "sql", "warehouses", "stop",
            warehouse_id,
            "--output", "json"
        ]
        return await self.execute(command_args)
    
    async def execute_query(
        self,
        query: str,
        warehouse_id: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query.
        
        Args:
            query: SQL query to execute
            warehouse_id: ID of the warehouse to use
            catalog: Catalog to use
            schema: Schema to use
            timeout: Query timeout in seconds
            
        Returns:
            Dictionary containing query results
        """
        logger.info("Executing SQL query")
        
        self.validate_required_args({"query": query}, ["query"])
        
        command_args = ["sql", "query"]
        
        if warehouse_id:
            command_args.extend(["--warehouse-id", warehouse_id])
        
        if catalog:
            command_args.extend(["--catalog", catalog])
        
        if schema:
            command_args.extend(["--schema", schema])
        
        if timeout:
            command_args.extend(["--timeout", str(timeout)])
        
        command_args.extend([
            "--query", query,
            "--output", "json"
        ])
        
        return await self.execute(command_args)
    
    async def execute_query_file(
        self,
        file_path: str,
        warehouse_id: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        timeout: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query from a file.
        
        Args:
            file_path: Path to the SQL file
            warehouse_id: ID of the warehouse to use
            catalog: Catalog to use
            schema: Schema to use
            timeout: Query timeout in seconds
            
        Returns:
            Dictionary containing query results
        """
        logger.info(f"Executing SQL query from file: {file_path}")
        
        self.validate_required_args({"file_path": file_path}, ["file_path"])
        
        command_args = ["sql", "query"]
        
        if warehouse_id:
            command_args.extend(["--warehouse-id", warehouse_id])
        
        if catalog:
            command_args.extend(["--catalog", catalog])
        
        if schema:
            command_args.extend(["--schema", schema])
        
        if timeout:
            command_args.extend(["--timeout", str(timeout)])
        
        command_args.extend([
            "--file", file_path,
            "--output", "json"
        ])
        
        return await self.execute(command_args)
    
    async def list_databases(
        self,
        catalog: Optional[str] = None,
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List databases/schemas.
        
        Args:
            catalog: Catalog to list databases from
            warehouse_id: Warehouse to use for the operation
            
        Returns:
            Dictionary containing list of databases
        """
        logger.info("Listing databases")
        
        query = "SHOW SCHEMAS"
        if catalog:
            query += f" IN {catalog}"
        
        return await self.execute_query(
            query=query,
            warehouse_id=warehouse_id
        )
    
    async def list_tables(
        self,
        database: Optional[str] = None,
        catalog: Optional[str] = None,
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List tables in a database.
        
        Args:
            database: Database to list tables from
            catalog: Catalog to use
            warehouse_id: Warehouse to use for the operation
            
        Returns:
            Dictionary containing list of tables
        """
        logger.info(f"Listing tables in database: {database}")
        
        query = "SHOW TABLES"
        if database:
            query += f" IN {database}"
        elif catalog:
            query += f" IN {catalog}"
        
        return await self.execute_query(
            query=query,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=database
        )
    
    async def describe_table(
        self,
        table_name: str,
        database: Optional[str] = None,
        catalog: Optional[str] = None,
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Describe a table structure.
        
        Args:
            table_name: Name of the table to describe
            database: Database containing the table
            catalog: Catalog containing the table
            warehouse_id: Warehouse to use for the operation
            
        Returns:
            Dictionary containing table structure
        """
        logger.info(f"Describing table: {table_name}")
        
        self.validate_required_args({"table_name": table_name}, ["table_name"])
        
        # Construct full table name
        full_table_name = table_name
        if database and catalog:
            full_table_name = f"{catalog}.{database}.{table_name}"
        elif database:
            full_table_name = f"{database}.{table_name}"
        
        query = f"DESCRIBE TABLE {full_table_name}"
        
        return await self.execute_query(
            query=query,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=database
        )
    
    async def create_database(
        self,
        database_name: str,
        catalog: Optional[str] = None,
        comment: Optional[str] = None,
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a database/schema.
        
        Args:
            database_name: Name of the database to create
            catalog: Catalog to create the database in
            comment: Comment for the database
            warehouse_id: Warehouse to use for the operation
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Creating database: {database_name}")
        
        self.validate_required_args({"database_name": database_name}, ["database_name"])
        
        query = f"CREATE SCHEMA IF NOT EXISTS {database_name}"
        if comment:
            query += f" COMMENT '{comment}'"
        
        return await self.execute_query(
            query=query,
            warehouse_id=warehouse_id,
            catalog=catalog
        )
    
    async def drop_database(
        self,
        database_name: str,
        catalog: Optional[str] = None,
        cascade: bool = False,
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Drop a database/schema.
        
        Args:
            database_name: Name of the database to drop
            catalog: Catalog containing the database
            cascade: Whether to drop cascade (delete all objects)
            warehouse_id: Warehouse to use for the operation
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Dropping database: {database_name}")
        
        self.validate_required_args({"database_name": database_name}, ["database_name"])
        
        query = f"DROP SCHEMA IF EXISTS {database_name}"
        if cascade:
            query += " CASCADE"
        else:
            query += " RESTRICT"
        
        return await self.execute_query(
            query=query,
            warehouse_id=warehouse_id,
            catalog=catalog
        )
    
    async def list_catalogs(self, warehouse_id: Optional[str] = None) -> Dict[str, Any]:
        """
        List available catalogs.
        
        Args:
            warehouse_id: Warehouse to use for the operation
            
        Returns:
            Dictionary containing list of catalogs
        """
        logger.info("Listing catalogs")
        
        query = "SHOW CATALOGS"
        
        return await self.execute_query(
            query=query,
            warehouse_id=warehouse_id
        )
    
    async def get_current_catalog(self, warehouse_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the current catalog.
        
        Args:
            warehouse_id: Warehouse to use for the operation
            
        Returns:
            Dictionary containing current catalog info
        """
        logger.info("Getting current catalog")
        
        query = "SELECT current_catalog()"
        
        return await self.execute_query(
            query=query,
            warehouse_id=warehouse_id
        )
    
    async def use_catalog(
        self,
        catalog_name: str,
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Switch to a different catalog.
        
        Args:
            catalog_name: Name of the catalog to use
            warehouse_id: Warehouse to use for the operation
            
        Returns:
            Dictionary containing operation result
        """
        logger.info(f"Using catalog: {catalog_name}")
        
        self.validate_required_args({"catalog_name": catalog_name}, ["catalog_name"])
        
        query = f"USE CATALOG {catalog_name}"
        
        return await self.execute_query(
            query=query,
            warehouse_id=warehouse_id
        )


# Create a global instance for easy importing
sql_cli = SQLCLI()
