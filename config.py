import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')  # e.g., 'abc12345.us-east-1'
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'DEMO_DB')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE', 'PUBLIC')
    
    # DuckDB Configuration
    DUCKDB_PATH = 'local_data.duckdb'
