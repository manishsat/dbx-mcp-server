# Databricks notebook source
# MAGIC %md
# MAGIC # Hello World MCP Notebook
# MAGIC 
# MAGIC This notebook demonstrates the complete MCP workflow:
# MAGIC 1. Notebook created and uploaded via MCP
# MAGIC 2. Job created pointing to this notebook
# MAGIC 3. Job executed successfully
# MAGIC 4. Results monitored via MCP tools

# COMMAND ----------

# Get parameters passed from job
dbutils.widgets.text("greeting", "Hello from MCP!")
dbutils.widgets.text("timestamp", "2025-09-15")
dbutils.widgets.text("created_by", "mcp_server")

greeting = dbutils.widgets.get("greeting")
timestamp = dbutils.widgets.get("timestamp")
created_by = dbutils.widgets.get("created_by")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Execution

# COMMAND ----------

import datetime
import json

# Print hello world message with parameters
print("=" * 60)
print("🎉 HELLO WORLD FROM MCP WORKFLOW! 🎉")
print("=" * 60)
print(f"📝 Greeting: {greeting}")
print(f"⏰ Timestamp: {timestamp}")
print(f"👤 Created by: {created_by}")
print(f"🕐 Execution time: {datetime.datetime.now()}")
print("=" * 60)

# COMMAND ----------

# Create some sample data processing
import pandas as pd

# Sample data
data = {
    'step': ['Notebook Upload', 'Job Creation', 'Job Execution', 'Result Monitoring'],
    'status': ['✅ Complete', '✅ Complete', '🔄 Running', '⏳ Pending'],
    'tool': ['create_notebook', 'create_job', 'run_job', 'get_job_run'],
    'timestamp': [datetime.datetime.now() - datetime.timedelta(minutes=i) for i in range(4)]
}

df = pd.DataFrame(data)
print("\n📊 MCP Workflow Status:")
print(df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Success Output

# COMMAND ----------

# Generate success output for job monitoring
result = {
    "status": "SUCCESS",
    "message": "Hello World notebook executed successfully via MCP!",
    "greeting": greeting,
    "timestamp": timestamp,
    "created_by": created_by,
    "execution_time": str(datetime.datetime.now()),
    "workflow_step": "notebook_execution_complete"
}

print("\n🎯 Final Result:")
print(json.dumps(result, indent=2))

# Store result for job monitoring
dbutils.jobs.taskValues.set(key="notebook_result", value=result)

print("\n✅ Notebook execution completed successfully!")
print("🚀 Ready for MCP monitoring tools to track status!")
