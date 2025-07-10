import snowflake.connector
from snowflake.connector import DictCursor
import duckdb
import pandas as pd
import logging
from config import Config
import sys
from typing import Optional, List, Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self):
        self.snowflake_conn = None
        self.duckdb_conn = None
        
    def connect_snowflake(self) -> bool:
        """Connect to Snowflake with proper error handling"""
        try:
            # Basic connection parameters
            connection_params = {
                'account': Config.SNOWFLAKE_ACCOUNT,
                'user': Config.SNOWFLAKE_USER,
                'password': Config.SNOWFLAKE_PASSWORD,
                'database': Config.SNOWFLAKE_DATABASE,
                'schema': Config.SNOWFLAKE_SCHEMA,
                'warehouse': Config.SNOWFLAKE_WAREHOUSE,
                'role': Config.SNOWFLAKE_ROLE,
                'application': 'SnowflakeToDuckDBPipeline',
                'login_timeout': 60,
                'network_timeout': 60,
            }
            
            # Try connection with SSL verification first
            try:
                self.snowflake_conn = snowflake.connector.connect(**connection_params)
                logger.info("Successfully connected to Snowflake with SSL verification")
                return True
            except Exception as ssl_error:
                logger.warning(f"SSL connection failed: {str(ssl_error)}")
                logger.info("Attempting connection with relaxed SSL settings...")
                
                # Fallback: try with insecure mode
                connection_params['insecure_mode'] = True
                self.snowflake_conn = snowflake.connector.connect(**connection_params)
                logger.info("Successfully connected to Snowflake with insecure mode")
                return True
                
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            return False
    
    def connect_duckdb(self) -> bool:
        """Connect to DuckDB (creates file if doesn't exist)"""
        try:
            self.duckdb_conn = duckdb.connect(Config.DUCKDB_PATH, read_only=False)
            logger.info(f"Successfully connected to DuckDB: {Config.DUCKDB_PATH}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {str(e)}")
            return False
    
    def get_snowflake_tables(self) -> List[str]:
        """Get available tables from Snowflake"""
        try:
            if not self.snowflake_conn:
                raise Exception("Snowflake connection not established")
            
            cursor = self.snowflake_conn.cursor()
            cursor.execute(f"SHOW TABLES IN SCHEMA {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}")
            tables = [row[1] for row in cursor.fetchall()]  # Table name is in second column
            cursor.close()
            
            logger.info(f"Found {len(tables)} tables in Snowflake")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to get table list: {str(e)}")
            return []
    
    def read_snowflake_data(self, query: str, limit: int = 1000) -> Optional[pd.DataFrame]:
        """Read data from Snowflake with proper error handling"""
        try:
            if not self.snowflake_conn:
                raise Exception("Snowflake connection not established")
            
            # Add limit to query if not present
            if 'LIMIT' not in query.upper():
                query = f"{query.rstrip(';')} LIMIT {limit}"
            
            logger.info(f"Executing query: {query[:100]}...")
            
            # Use cursor for better memory management
            cursor = self.snowflake_conn.cursor(DictCursor)
            cursor.execute(query)
            
            # Fetch results
            results = cursor.fetchall()
            
            if not results:
                logger.warning("Query returned no results")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            cursor.close()
            
            logger.info(f"Successfully read {len(df)} rows with {len(df.columns)} columns from Snowflake")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read from Snowflake: {str(e)}")
            raise
    
    def write_to_duckdb(self, df: pd.DataFrame, table_name: str) -> bool:
        """Write pandas DataFrame to DuckDB with proper data type handling"""
        try:
            if not self.duckdb_conn:
                raise Exception("DuckDB connection not established")
            
            if df.empty:
                logger.warning("DataFrame is empty, skipping write")
                return False
            
            # Clean table name (remove special characters)
            clean_table_name = table_name.replace('-', '_').replace(' ', '_').replace('.', '_')
            
            # Drop table if exists
            self.duckdb_conn.execute(f"DROP TABLE IF EXISTS {clean_table_name}")
            
            # Create table from DataFrame
            self.duckdb_conn.register(f'{clean_table_name}_temp', df)
            self.duckdb_conn.execute(f"CREATE TABLE {clean_table_name} AS SELECT * FROM {clean_table_name}_temp")
            self.duckdb_conn.unregister(f'{clean_table_name}_temp')
            
            # Verify the write
            count = self.duckdb_conn.execute(f"SELECT COUNT(*) FROM {clean_table_name}").fetchone()[0]
            
            logger.info(f"Successfully wrote {count} rows to DuckDB table: {clean_table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write to DuckDB: {str(e)}")
            raise
    
    def run_pipeline(self, table_name: Optional[str] = None, custom_query: Optional[str] = None, limit: int = 1000) -> Optional[pd.DataFrame]:
        """Run the complete pipeline with better error handling"""
        try:
            # Connect to databases
            logger.info("=== Starting Data Pipeline ===")
            
            if not self.connect_snowflake():
                raise Exception("Could not connect to Snowflake")
                
            if not self.connect_duckdb():
                raise Exception("Could not connect to DuckDB")
            
            # Determine what to query
            if custom_query:
                query = custom_query
                duckdb_table_name = "custom_query_result"
            elif table_name:
                query = f"SELECT * FROM {table_name}"
                duckdb_table_name = table_name.lower()
            else:
                # Get first available table
                tables = self.get_snowflake_tables()
                if not tables:
                    raise Exception("No tables found in Snowflake schema")
                
                table_name = tables[0]
                query = f"SELECT * FROM {table_name}"
                duckdb_table_name = table_name.lower()
                logger.info(f"Using first available table: {table_name}")
            
            # Read from Snowflake
            logger.info("Starting data extraction from Snowflake...")
            df = self.read_snowflake_data(query, limit)
            
            if df is None or df.empty:
                logger.warning("No data retrieved from Snowflake")
                return None
            
            # Write to DuckDB
            logger.info("Starting data load to DuckDB...")
            success = self.write_to_duckdb(df, duckdb_table_name)
            
            if success:
                logger.info("=== Pipeline completed successfully! ===")
                return df
            else:
                raise Exception("Failed to write data to DuckDB")
                
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            # Clean up connections
            self.cleanup()
    
    def cleanup(self):
        """Clean up database connections"""
        if self.snowflake_conn:
            try:
                self.snowflake_conn.close()
                logger.info("Snowflake connection closed")
            except:
                pass
        if self.duckdb_conn:
            try:
                self.duckdb_conn.close()
                logger.info("DuckDB connection closed")
            except:
                pass