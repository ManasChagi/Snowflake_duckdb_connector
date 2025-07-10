# test_connection.py - Test script to verify your setup
import os
from dotenv import load_dotenv
import snowflake.connector

def test_snowflake_connection():
    """Test Snowflake connection independently"""
    load_dotenv()
    
    try:
        print("Testing Snowflake connection...")
        
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE', 'DEMO_DB'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            role=os.getenv('SNOWFLAKE_ROLE', 'PUBLIC'),
            insecure_mode=True  # Add this if you get SSL errors
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()[0]
        print(f"✅ Successfully connected to Snowflake!")
        print(f"📊 Snowflake version: {version}")
        
        # Test table access
        cursor.execute(f"SHOW TABLES IN SCHEMA {os.getenv('SNOWFLAKE_DATABASE', 'DEMO_DB')}.{os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')}")
        tables = cursor.fetchall()
        print(f"📋 Found {len(tables)} tables in schema")
        
        if tables:
            print("Available tables:")
            for table in tables[:5]:  # Show first 5 tables
                print(f"  - {table[1]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_snowflake_connection()