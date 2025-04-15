from fastapi import FastAPI, HTTPException, Depends, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import clickhouse_driver
import pandas as pd
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="ClickHouse-FlatFile Data Ingestion Tool")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ClickHouseConnection(BaseModel):
    host: str
    port: int
    database: str
    user: str
    jwt_token: str

class ColumnSelection(BaseModel):
    columns: List[str]
    table: str

class JoinConfig(BaseModel):
    tables: List[str]
    join_conditions: List[str]

def get_clickhouse_client(conn: ClickHouseConnection):
    try:
        client = clickhouse_driver.Client(
            host=conn.host,
            port=conn.port,
            database=conn.database,
            user=conn.user,
            password=conn.jwt_token,
            secure=True if conn.port in [9440, 8443] else False
        )
        return client
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")

@app.post("/api/connect")
async def connect_to_clickhouse(conn: ClickHouseConnection):
    try:
        client = get_clickhouse_client(conn)
        # Test connection
        client.execute("SELECT 1")
        return {"status": "success", "message": "Connected successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/tables")
async def get_tables(conn: ClickHouseConnection):
    try:
        client = get_clickhouse_client(conn)
        tables = client.execute("SHOW TABLES")
        return {"tables": [table[0] for table in tables]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/columns")
async def get_columns(conn: ClickHouseConnection, table: str):
    try:
        client = get_clickhouse_client(conn)
        columns = client.execute(f"DESCRIBE TABLE {table}")
        return {"columns": [{"name": col[0], "type": col[1]} for col in columns]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/ingest/clickhouse-to-file")
async def ingest_clickhouse_to_file(
    conn: ClickHouseConnection,
    selection: ColumnSelection,
    file_format: str = "csv"
):
    try:
        client = get_clickhouse_client(conn)
        columns_str = ", ".join(selection.columns)
        query = f"SELECT {columns_str} FROM {selection.table}"
        
        # Execute query and get data
        data = client.execute(query, with_column_types=True)
        
        # Convert to DataFrame
        df = pd.DataFrame(data[0], columns=[col[0] for col in data[1]])
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"export_{selection.table}_{timestamp}.{file_format}"
        
        # Save to file
        if file_format == "csv":
            df.to_csv(filename, index=False)
        elif file_format == "json":
            df.to_json(filename, orient="records")
        
        return {
            "status": "success",
            "filename": filename,
            "record_count": len(df)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/ingest/file-to-clickhouse")
async def ingest_file_to_clickhouse(
    conn: ClickHouseConnection,
    file: UploadFile = File(...),
    table: str = None,
    columns: List[str] = None
):
    try:
        # Read file content
        content = await file.read()
        
        # Determine file type and read into DataFrame
        if file.filename.endswith('.csv'):
            df = pd.read_csv(pd.io.common.BytesIO(content))
        elif file.filename.endswith('.json'):
            df = pd.read_json(pd.io.common.BytesIO(content))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")
        
        # Filter columns if specified
        if columns:
            df = df[columns]
        
        # Generate table name if not provided
        if not table:
            table = f"imported_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Connect to ClickHouse
        client = get_clickhouse_client(conn)
        
        # Create table if it doesn't exist
        columns_def = ", ".join([f"{col} String" for col in df.columns])
        client.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns_def}) ENGINE = MergeTree() ORDER BY tuple()")
        
        # Insert data
        data = df.values.tolist()
        client.execute(f"INSERT INTO {table} VALUES", data)
        
        return {
            "status": "success",
            "table": table,
            "record_count": len(df)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/join-tables")
async def join_tables(
    conn: ClickHouseConnection,
    join_config: JoinConfig
):
    try:
        client = get_clickhouse_client(conn)
        
        # Construct JOIN query
        tables = join_config.tables
        conditions = join_config.join_conditions
        
        query = f"SELECT * FROM {tables[0]}"
        for i, table in enumerate(tables[1:], 1):
            query += f" JOIN {table} ON {conditions[i-1]}"
        
        # Execute query
        data = client.execute(query, with_column_types=True)
        
        # Convert to DataFrame
        df = pd.DataFrame(data[0], columns=[col[0] for col in data[1]])
        
        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"joined_export_{timestamp}.csv"
        df.to_csv(filename, index=False)
        
        return {
            "status": "success",
            "filename": filename,
            "record_count": len(df)
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 