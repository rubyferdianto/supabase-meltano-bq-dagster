"""
Clean implementation of dim_date asset using dbt SQL file
"""

import os
import subprocess
from typing import Dict, Any
from dotenv import load_dotenv

from dagster import (
    asset, 
    get_dagster_logger,
    Config
)


class PipelineConfig(Config):
    """Configuration for the pipeline"""
    staging_bigquery_dataset: str = os.getenv("TARGET_STAGING_DATASET", "olist_data_staging")
    bigquery_dataset: str = os.getenv("TARGET_BIGQUERY_DATASET", "olist_data_warehouse")


@asset(group_name="Transformation")
def _2a_processing_dim_date(config: PipelineConfig) -> Dict[str, Any]:
    """
    Process and create dimension table for dates using dbt SQL file
    
    Creates dim_date table using the separate SQL file with:
    - date_sk (primary key - auto-incrementing)
    - date_value (date not null)
    - year, quarter, month, day_of_month, day_of_week (bigint)
    - is_weekend (boolean)
    - Additional attributes: day_name, month_name, week_of_year, is_business_day
    
    The table will be created in olist_data_warehouse.dimensions_dim_date
    
    Returns:
        Date dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_date using dbt SQL file")
    logger.info(f"Reading from staging dataset: {config.staging_bigquery_dataset}")
    logger.info(f"Writing to production dataset: {config.bigquery_dataset}")
    
    # dbt directory
    dbt_dir = "/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/bec_dbt"
    
    try:
        # Load environment variables from .env file
        load_dotenv('/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/.env')

        # Set environment variables for dbt
        env_vars = os.environ.copy()
        env_vars.update({
            'TARGET_BIGQUERY_DATASET': config.bigquery_dataset,
            'TARGET_STAGING_DATASET': config.staging_bigquery_dataset,
        })
        
        logger.info("🔄 Running dbt model: dim_date...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_date.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_date model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --select dim_date --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_date model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_date model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_date model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_date' in line and ('OK created' in line or 'OK' in line):
                model_created = True
                logger.info(f"   ✅ {line.strip()}")
            elif 'rows affected' in line.lower():
                try:
                    # Try to extract row count from dbt output
                    import re
                    match = re.search(r'(\d+)', line)
                    if match:
                        records_processed = int(match.group(1))
                except:
                    pass
        
        if not model_created:
            logger.warning("⚠️ Could not confirm dim_date model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dimensions_dim_date")
                actual_records = table_ref.num_rows
                
                logger.info(f"✅ Verified table in BigQuery: {actual_records:,} records")
                records_processed = actual_records
                
                # Get schema info
                schema_fields = [field.name for field in table_ref.schema]
                logger.info(f"📋 Table schema: {', '.join(schema_fields)}")
                
        except Exception as verify_error:
            logger.warning(f"⚠️ Could not verify table in BigQuery: {str(verify_error)}")
            logger.info("💡 Table may still have been created successfully")
        
        result = {
            "table_name": "dim_date", 
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dimensions_dim_date",
            "dbt_model": "dim_date",
            "sql_file": "models/marts/dimensions/dim_date.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Date dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_date model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_date model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
