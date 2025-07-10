import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from config import Config
from data_pipeline import DataPipeline
import os
from typing import Tuple, Optional, List, Dict

def check_environment() -> Tuple[bool, List[str]]:
    """Check if environment is properly configured"""
    issues = []
    
    # Check required environment variables
    required_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
    for var in required_vars:
        if not os.getenv(var):
            issues.append(f"Missing environment variable: {var}")
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        issues.append("Missing .env file in project directory")
    
    return len(issues) == 0, issues

def load_data_from_duckdb() -> Tuple[Optional[pd.DataFrame], List[str], Dict[str, int]]:
    """Load data from DuckDB for display"""
    try:
        if not os.path.exists(Config.DUCKDB_PATH):
            return None, [], {}
        
        conn = duckdb.connect(Config.DUCKDB_PATH, read_only=True)
        
        # Get list of tables with row counts
        tables_query = """
        SELECT table_name, 
               (SELECT COUNT(*) FROM """ + Config.DUCKDB_PATH.replace('.duckdb', '') + """.main.""" + """|| table_name) as row_count
        FROM information_schema.tables 
        WHERE table_schema = 'main'
        """
        
        try:
            # Simpler approach - just get table names
            tables_df = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").fetchdf()
            
            if len(tables_df) == 0:
                conn.close()
                return None, [], {}
            
            table_names = tables_df['table_name'].tolist()
            
            # Get row counts for each table
            table_stats = {}
            for table in table_names:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    table_stats[table] = count
                except:
                    table_stats[table] = 0
            
            # Get data from the first table
            first_table = table_names[0]
            data_df = conn.execute(f"SELECT * FROM {first_table} LIMIT 1000").fetchdf()
            
            conn.close()
            return data_df, table_names, table_stats
            
        except Exception as e:
            conn.close()
            st.error(f"Error querying DuckDB: {str(e)}")
            return None, [], {}
        
    except Exception as e:
        st.error(f"Error connecting to DuckDB: {str(e)}")
        return None, [], {}

def run_data_pipeline(table_name: Optional[str] = None, limit: int = 1000) -> bool:
    """Run the data pipeline from Streamlit interface"""
    try:
        pipeline = DataPipeline()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        status_text.text("Connecting to Snowflake...")
        progress_bar.progress(20)
        
        # Run pipeline
        df = pipeline.run_pipeline(table_name=table_name, limit=limit)
        
        progress_bar.progress(100)
        status_text.text("Pipeline completed successfully!")
        
        if df is not None:
            st.success(f"✅ Successfully processed {len(df)} rows!")
            return True
        else:
            st.warning("⚠️ Pipeline completed but no data was processed")
            return False
        
    except Exception as e:
        st.error(f"❌ Pipeline failed: {str(e)}")
        
        # Show troubleshooting tips
        with st.expander("🔧 Troubleshooting Tips"):
            st.write("**Common Issues:**")
            st.write("1. **SSL Errors**: Usually resolved automatically with fallback connection")
            st.write("2. **Authentication**: Check your Snowflake credentials in .env file")
            st.write("3. **Table Access**: Ensure your user has access to the specified table/schema")
            st.write("4. **Network**: Check if you can access Snowflake from your network")
            
        return False

def main():
    st.set_page_config(
        page_title="Snowflake → DuckDB → Streamlit Pipeline",
        page_icon="🏔️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Header
    st.title("🏔️ Snowflake → DuckDB → Streamlit Pipeline")
    st.markdown("**Extract data from Snowflake, store locally in DuckDB, and visualize with Streamlit**")
    
    # Check environment
    env_ok, env_issues = check_environment()
    
    if not env_ok:
        st.error("❌ Environment Configuration Issues:")
        for issue in env_issues:
            st.write(f"• {issue}")
        
        with st.expander("📋 Setup Instructions"):
            st.markdown("""
            **Create a `.env` file in your project directory with:**
            ```
            SNOWFLAKE_ACCOUNT=your_account_identifier
            SNOWFLAKE_USER=your_username
            SNOWFLAKE_PASSWORD=your_password
            SNOWFLAKE_DATABASE=your_database
            SNOWFLAKE_SCHEMA=PUBLIC
            SNOWFLAKE_WAREHOUSE=COMPUTE_WH
            SNOWFLAKE_ROLE=PUBLIC
            ```
            
            **Find your Snowflake account identifier:**
            - Login to Snowflake web interface
            - Look at URL: `https://abc12345.us-east-1.snowflakecomputing.com`
            - Your account is: `abc12345.us-east-1`
            """)
        return
    
    # Sidebar controls
    st.sidebar.header("🚀 Pipeline Controls")
    
    # Pipeline options
    st.sidebar.subheader("Options")
    limit = st.sidebar.slider("Row Limit", min_value=100, max_value=10000, value=1000, step=100)
    table_name = st.sidebar.text_input("Specific Table Name (optional)", placeholder="e.g., CUSTOMERS")
    
    # Run pipeline button
    if st.sidebar.button("🚀 Run Data Pipeline", type="primary", use_container_width=True):
        with st.spinner("Running pipeline..."):
            run_data_pipeline(table_name if table_name else None, limit)
    
    st.sidebar.markdown("---")
    
    # Pipeline status
    st.sidebar.subheader("📊 Pipeline Status")
    duckdb_exists = os.path.exists(Config.DUCKDB_PATH)
    st.sidebar.write(f"DuckDB File: {'✅ Exists' if duckdb_exists else '❌ Not Found'}")
    
    if duckdb_exists:
        file_size = os.path.getsize(Config.DUCKDB_PATH) / (1024 * 1024)  # MB
        st.sidebar.write(f"File Size: {file_size:.2f} MB")
    
    # Main content
    st.header("📈 Data Dashboard")
    
    # Load and display data
    data_df, available_tables, table_stats = load_data_from_duckdb()
    
    if data_df is not None and not data_df.empty:
        # Success metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Total Rows", len(data_df))
        with col2:
            st.metric("📋 Columns", len(data_df.columns))
        with col3:
            st.metric("🗂️ Tables", len(available_tables))
        with col4:
            total_rows = sum(table_stats.values())
            st.metric("📈 Total Rows (All Tables)", total_rows)
        
        # Table information
        if len(available_tables) > 1:
            st.subheader("📋 Available Tables")
            table_info = pd.DataFrame([
                {"Table Name": table, "Row Count": count} 
                for table, count in table_stats.items()
            ])
            st.dataframe(table_info, use_container_width=True)
        
        # Data preview
        st.subheader("🔍 Data Preview")
        st.dataframe(data_df.head(50), use_container_width=True)
        
        # Data analysis
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Data Info")
            info_df = pd.DataFrame({
                'Column': data_df.columns,
                'Data Type': [str(dtype) for dtype in data_df.dtypes],
                'Non-Null Count': [data_df[col].notna().sum() for col in data_df.columns],
                'Null Count': [data_df[col].isna().sum() for col in data_df.columns]
            })
            st.dataframe(info_df, use_container_width=True)
        
        with col2:
            st.subheader("🔢 Numeric Summary")
            numeric_df = data_df.select_dtypes(include=['number'])
            if not numeric_df.empty:
                st.dataframe(numeric_df.describe(), use_container_width=True)
            else:
                st.info("No numeric columns found")
        
        # Visualizations
        st.subheader("📈 Quick Visualizations")
        
        # Only show visualizations if we have data
        numeric_columns = data_df.select_dtypes(include=['number']).columns.tolist()
        categorical_columns = data_df.select_dtypes(include=['object', 'string']).columns.tolist()
        
        if len(numeric_columns) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                if len(numeric_columns) >= 1:
                    selected_numeric = st.selectbox("Select numeric column:", numeric_columns)
                    fig = px.histogram(data_df, x=selected_numeric, title=f"Distribution of {selected_numeric}")
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if len(categorical_columns) > 0 and len(numeric_columns) > 0:
                    selected_cat = st.selectbox("Select categorical column:", categorical_columns)
                    fig = px.box(data_df, x=selected_cat, y=selected_numeric, title=f"{selected_numeric} by {selected_cat}")
                    st.plotly_chart(fig, use_container_width=True)
        
        # Raw data download
        st.subheader("💾 Export Data")
        csv = data_df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"snowflake_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    else:
        # No data state
        st.info("🔄 No data found in DuckDB. Run the pipeline to get started!")
        
        st.markdown("""
        ### 🚀 Getting Started
        
        1. **Configure Environment**: Make sure your `.env` file is set up with Snowflake credentials
        2. **Run Pipeline**: Click the "🚀 Run Data Pipeline" button in the sidebar
        3. **Wait for Completion**: The pipeline will extract data from Snowflake and store it locally
        4. **Explore Data**: Once complete, you'll see your data visualized here
        
        ### 💡 What This Pipeline Does
        
        - **Extracts** data from your Snowflake data warehouse
        - **Stores** it locally in DuckDB (fast analytical database)
        - **Visualizes** the data with interactive charts and tables
        - **Enables** offline analysis without additional Snowflake costs
        """)

if __name__ == "__main__":
    main()
