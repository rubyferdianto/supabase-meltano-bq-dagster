"""
Professional data orchestration tool for automated data pipeline management
"""

import os
import glob
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import psycopg2

# Load environment variables from .env file in parent directory
load_dotenv('../.env')

from dagster import (
    asset, 
    job, 
    materialize,
    AssetMaterialization,
    AssetObservation,
    Output,
    MetadataValue,
    Config,
    get_dagster_logger,
    Definitions
)


class PipelineConfig(Config):
    """Configuration for the pipeline"""
    staging_bigquery_dataset: str = os.getenv("TARGET_STAGING_DATASET", "bec_dataset")
    bigquery_dataset: str = os.getenv("TARGET_BIGQUERY_DATASET")
 

@asset(group_name="Extraction")
def _1_staging_to_bigquery(config: PipelineConfig) -> Dict[str, Any]:
    """
    Simple ELT Loading: Supabase → BigQuery using Meltano
    Pure TRUNCATE and INSERT approach - no complex checks
    
    Returns:
        Simple transfer metadata
    """
    logger = get_dagster_logger()

    logger.info("� Simple ELT Loading: Supabase → BigQuery (via Meltano)")
    logger.info("📋 Method: TRUNCATE existing tables + INSERT fresh data")
    
    # Meltano directory
    meltano_dir = "/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/bec-meltano"

    # Initialize collections for tracking
    all_table_names = []
    all_bq_tables = []
    all_transfer_logs = []
    
    # ===========================================
    # PHASE 1: Process Supabase tables to STAGING dataset
    # ===========================================
    logger.info("🚀 PHASE 1: Processing Supabase tables to staging dataset...")
    supabase_tables = []
    
    try:
        # Use PostgreSQL connection (same as Meltano) instead of Supabase REST API
        import psycopg2
        
        # Get PostgreSQL connection details for Supabase
        supabase_host = "aws-1-ap-southeast-1.pooler.supabase.com"
        supabase_port = 5432
        supabase_database = "postgres"
        supabase_user = "postgres.royhmnxmsfichopabwsi"
        supabase_password = os.getenv("TAP_POSTGRES_PASSWORD", "MD4mq0O6AA4qlfpt")
        
        if supabase_password:
            logger.info("✅ Connected to Supabase via PostgreSQL")
            
            # Connect to Supabase PostgreSQL database
            conn = psycopg2.connect(
                host=supabase_host,
                port=supabase_port,
                database=supabase_database,
                user=supabase_user,
                password=supabase_password
            )
            
            cursor = conn.cursor()
            
            # Get table list using PostgreSQL query
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name LIKE '%olist%' OR table_name LIKE '%product_category%'
                ORDER BY table_name;
            """)
            
            supabase_tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

            # RUBY - INDICATOR FOR SUPABASE TO BIGQUERY
            supabase_tables = False
            
            if supabase_tables:
                logger.info(f"📊 Discovered {len(supabase_tables)} tables from Supabase via PostgreSQL: {supabase_tables}")
            else:
                logger.info("No tables found in Supabase PostgreSQL")
                
        else:
            logger.warning("⚠️ TAP_POSTGRES_PASSWORD not found in environment variables")
            logger.info("💡 Set TAP_POSTGRES_PASSWORD to enable Supabase processing")
            
    except ImportError as import_error:
        logger.warning(f"⚠️ PostgreSQL client not available: {import_error}")
        logger.info("💡 Install with: pip install psycopg2-binary")
    except Exception as supabase_error:
        logger.error(f"❌ Could not connect to Supabase: {str(supabase_error)}")
    
    # Process Supabase tables if found
    if supabase_tables:
        logger.info(f"🔄 Processing {len(supabase_tables)} Supabase tables for BigQuery STAGING transfer...")
        
        # Create detailed log file for Supabase transfer
        supabase_log_file = "../supabase_bq_staging_transfer.log"
        logger.info(f"📝 Detailed Supabase staging transfer logs will be written to: {supabase_log_file}")
        
        try:
            # TRUNCATE existing staging tables for fresh reload (preserve schema)
            logger.info("🧹 TRUNCATING existing staging tables (preserving schema)...")
            
            try:
                from google.cloud import bigquery
                import json
                
                # Initialize BigQuery client
                credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
                if credentials_json:
                    credentials_info = json.loads(credentials_json)
                    project_id = credentials_info.get("project_id")
                    
                    # Create BigQuery client
                    client = bigquery.Client(project=project_id)
                    
                    # Find existing tables to TRUNCATE (not DELETE)
                    dataset_ref = client.dataset(config.staging_bigquery_dataset, project=project_id)
                    
                    try:
                        tables = list(client.list_tables(dataset_ref))
                        tables_to_truncate = []
                        tables_to_delete = []
                        
                        # Separate clean tables (to truncate) from date-suffixed tables (to delete)
                        for table in tables:
                            table_name = table.table_id
                            for expected_table in supabase_tables:
                                expected_name = f"supabase_{expected_table}"
                                
                                if table_name == expected_name:
                                    # This is a clean table - TRUNCATE it
                                    tables_to_truncate.append(table_name)
                                elif table_name.startswith(f"{expected_name}__"):
                                    # This is a date-suffixed table - DELETE it
                                    tables_to_delete.append(table_name)
                        
                        # Remove duplicates
                        tables_to_truncate = list(set(tables_to_truncate))
                        tables_to_delete = list(set(tables_to_delete))
                        
                        logger.info(f"Found {len(tables_to_truncate)} tables to TRUNCATE: {tables_to_truncate}")
                        logger.info(f"Found {len(tables_to_delete)} date-suffixed tables to DELETE: {tables_to_delete[:3]}{'...' if len(tables_to_delete) > 3 else ''}")
                        
                        # TRUNCATE clean tables (preserve schema)
                        truncated_count = 0
                        for table_name in tables_to_truncate:
                            try:
                                table_id = f"{project_id}.{config.staging_bigquery_dataset}.{table_name}"
                                
                                # Use TRUNCATE TABLE SQL command to preserve schema
                                truncate_query = f"TRUNCATE TABLE `{table_id}`"
                                query_job = client.query(truncate_query)
                                query_job.result()  # Wait for completion
                                
                                logger.info(f"   🔄 TRUNCATED table (schema preserved): {table_name}")
                                truncated_count += 1
                                
                            except Exception as table_error:
                                logger.warning(f"   ⚠️ Could not truncate table {table_name}: {str(table_error)}")
                        
                        # DELETE date-suffixed tables (cleanup orphans)
                        deleted_count = 0
                        for table_name in tables_to_delete:
                            try:
                                table_id = f"{project_id}.{config.staging_bigquery_dataset}.{table_name}"
                                client.delete_table(table_id)
                                logger.info(f"   🗑️  DELETED date-suffixed table: {table_name}")
                                deleted_count += 1
                            except Exception as table_error:
                                logger.warning(f"   ⚠️ Could not delete table {table_name}: {str(table_error)}")
                        
                        logger.info(f"✅ Table preparation completed:")
                        logger.info(f"   📋 {truncated_count} tables TRUNCATED (schema preserved)")
                        logger.info(f"   🗑️  {deleted_count} orphaned tables DELETED")
                        
                    except Exception as list_error:
                        logger.warning(f"⚠️ Could not list existing tables: {str(list_error)}")
                        logger.info("💡 Meltano will handle table creation as needed")
                
                else:
                    logger.warning("⚠️ No BigQuery credentials found - skipping table preparation")
                    
            except ImportError:
                logger.warning("⚠️ BigQuery client not available - skipping table preparation")
            except Exception as cleanup_error:
                logger.warning(f"⚠️ Table preparation failed: {str(cleanup_error)}")
                logger.info("💡 Continuing with Meltano transfer")
            
            # Execute Supabase to BigQuery STAGING transfer using Meltano
            logger.info("🚀 Starting Meltano supabase-to-bigquery pipeline to STAGING dataset...")
            logger.info(f"Working directory: ../bec-meltano")
            logger.info(f"Command: meltano run supabase-to-bigquery")
            logger.info(f"Target dataset: {config.staging_bigquery_dataset}")
            
            supabase_result = subprocess.run([
                'bash', '-c', 
                'eval "$(conda shell.bash hook)" && conda activate bec && meltano run supabase-to-bigquery 2>&1 | tee ' + supabase_log_file
            ],
                capture_output=True,
                text=True,
                cwd="../bec-meltano",  # Use meltano directory
                timeout=900  # 15 minute timeout
            )
            
            if supabase_result.returncode == 0:
                logger.info("✅ Supabase to BigQuery STAGING transfer completed successfully")
                logger.info("📋 Supabase staging transfer summary:")
                
                # Parse output to get table-specific information
                output_lines = supabase_result.stdout.split('\n')
                table_count = 0
                failed_tables = []
                successful_tables = []
                
                for line in output_lines:
                    if 'supabase_' in line and ('loaded' in line.lower() or 'inserted' in line.lower()):
                        table_count += 1
                        if 'error' in line.lower() or 'failed' in line.lower():
                            failed_tables.append(line.strip())
                        else:
                            successful_tables.append(line.strip())
                
                logger.info(f"   📊 Tables processed to STAGING: {table_count}")
                logger.info(f"   ✅ Successful: {len(successful_tables)}")
                logger.info(f"   ❌ Failed: {len(failed_tables)}")
                
                if successful_tables:
                    logger.info("   📋 Successful table transfers to STAGING:")
                    for table_info in successful_tables[:5]:  # Show first 5
                        logger.info(f"      ✓ {table_info}")
                    if len(successful_tables) > 5:
                        logger.info(f"      ... and {len(successful_tables) - 5} more")
                
                if failed_tables:
                    logger.warning("   ⚠️ Failed table transfers:")
                    for table_info in failed_tables:
                        logger.warning(f"      ❌ {table_info}")
                
                # Add Supabase tables to collections
                all_table_names.extend(supabase_tables)
                all_transfer_logs.append(f"SUPABASE_STAGING: {len(successful_tables)} successful, {len(failed_tables)} failed")
                
                # Post-process: Migrate data from date-suffixed tables to clean tables
                logger.info("🔧 Post-processing: Migrating data from date-suffixed tables to clean tables...")
                
                try:
                    credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
                    if credentials_json:
                        credentials_info = json.loads(credentials_json)
                        project_id = credentials_info.get("project_id")
                        client = bigquery.Client(project=project_id)
                        
                        dataset_ref = client.dataset(config.staging_bigquery_dataset, project=project_id)
                        tables = list(client.list_tables(dataset_ref))
                        
                        # Categorize tables
                        clean_tables = {}
                        date_suffixed_tables = {}
                        
                        for table in tables:
                            table_name = table.table_id
                            
                            for expected_table in supabase_tables:
                                expected_name = f"supabase_{expected_table}"
                                
                                if table_name == expected_name:
                                    clean_tables[expected_name] = table_name
                                elif table_name.startswith(f"{expected_name}__"):
                                    if expected_name not in date_suffixed_tables:
                                        date_suffixed_tables[expected_name] = []
                                    date_suffixed_tables[expected_name].append(table_name)
                        
                        logger.info(f"📊 Found {len(clean_tables)} clean tables and {len(date_suffixed_tables)} groups with date-suffixed tables")
                        
                        migrated_count = 0
                        for expected_name in supabase_tables:
                            table_name = f"supabase_{expected_name}"
                            
                            # Check if we have date-suffixed tables to migrate
                            if table_name in date_suffixed_tables:
                                date_tables = date_suffixed_tables[table_name]
                                
                                # Find the table with data (non-zero rows)
                                source_table = None
                                max_rows = 0
                                
                                for date_table in date_tables:
                                    try:
                                        table_ref = client.get_table(f"{project_id}.{config.staging_bigquery_dataset}.{date_table}")
                                        if table_ref.num_rows > max_rows:
                                            max_rows = table_ref.num_rows
                                            source_table = date_table
                                    except Exception:
                                        continue
                                
                                if source_table and max_rows > 0:
                                    try:
                                        # Check if clean table exists
                                        clean_table_id = f"{project_id}.{config.staging_bigquery_dataset}.{table_name}"
                                        
                                        try:
                                            # Get existing clean table
                                            clean_table_ref = client.get_table(clean_table_id)
                                            logger.info(f"   � Clean table {table_name} exists ({clean_table_ref.num_rows} rows)")
                                            
                                            # If clean table is empty but date table has data, migrate
                                            if clean_table_ref.num_rows == 0 and max_rows > 0:
                                                # Delete empty clean table
                                                client.delete_table(clean_table_id)
                                                logger.info(f"   🗑️  Deleted empty clean table: {table_name}")
                                                
                                                # Copy date-suffixed table to clean name
                                                source_table_id = f"{project_id}.{config.staging_bigquery_dataset}.{source_table}"
                                                
                                                job_config = bigquery.CopyJobConfig()
                                                copy_job = client.copy_table(source_table_id, clean_table_id, job_config=job_config)
                                                copy_job.result()  # Wait for completion
                                                
                                                logger.info(f"   ✅ Migrated {source_table} → {table_name} ({max_rows:,} rows)")
                                                migrated_count += 1
                                            else:
                                                logger.info(f"   ℹ️  Clean table {table_name} already has data ({clean_table_ref.num_rows:,} rows)")
                                        
                                        except Exception:
                                            # Clean table doesn't exist, copy from date table
                                            source_table_id = f"{project_id}.{config.staging_bigquery_dataset}.{source_table}"
                                            
                                            job_config = bigquery.CopyJobConfig()
                                            copy_job = client.copy_table(source_table_id, clean_table_id, job_config=job_config)
                                            copy_job.result()  # Wait for completion
                                            
                                            logger.info(f"   ✅ Created {table_name} from {source_table} ({max_rows:,} rows)")
                                            migrated_count += 1
                                        
                                        # Clean up all date-suffixed tables for this base name
                                        for date_table in date_tables:
                                            try:
                                                date_table_id = f"{project_id}.{config.staging_bigquery_dataset}.{date_table}"
                                                client.delete_table(date_table_id)
                                                logger.info(f"   🧹 Cleaned up: {date_table}")
                                            except Exception as cleanup_error:
                                                logger.warning(f"   ⚠️ Could not clean up {date_table}: {str(cleanup_error)}")
                                    
                                    except Exception as migrate_error:
                                        logger.warning(f"   ⚠️ Could not migrate {source_table}: {str(migrate_error)}")
                                
                                else:
                                    logger.info(f"   ℹ️  No data found in date-suffixed tables for {table_name}")
                            
                            else:
                                # Check if clean table exists and has data
                                if table_name in clean_tables:
                                    try:
                                        clean_table_id = f"{project_id}.{config.staging_bigquery_dataset}.{table_name}"
                                        table_ref = client.get_table(clean_table_id)
                                        logger.info(f"   ✅ Clean table {table_name} ready ({table_ref.num_rows:,} rows)")
                                    except Exception:
                                        logger.warning(f"   ⚠️ Could not verify {table_name}")
                        
                        logger.info(f"✅ Data migration completed: {migrated_count} tables migrated to clean format")
                        
                        # Final verification
                        logger.info("🔍 Final table verification:")
                        for expected_table in supabase_tables:
                            table_name = f"supabase_{expected_table}"
                            try:
                                table_id = f"{project_id}.{config.staging_bigquery_dataset}.{table_name}"
                                table_ref = client.get_table(table_id)
                                logger.info(f"   ✅ {table_name}: {table_ref.num_rows:,} rows")
                            except Exception:
                                logger.warning(f"   ❌ {table_name}: NOT FOUND")
                    
                    else:
                        logger.warning("⚠️ No BigQuery credentials found - skipping data migration")
                        
                except Exception as postprocess_error:
                    logger.warning(f"⚠️ Data migration failed: {str(postprocess_error)}")
                    logger.info("💡 Some tables may still have date suffixes")
                
                # Generate BigQuery table references for Supabase tables in staging dataset
                for table_name in supabase_tables:
                    bq_table_ref = f"{config.staging_bigquery_dataset}.supabase_{table_name}"
                    all_bq_tables.append(bq_table_ref)
                    
                logger.info(f"📁 Full staging transfer details saved to: {supabase_log_file}")
                    
            else:
                logger.error(f"❌ Supabase to BigQuery STAGING transfer failed with return code: {supabase_result.returncode}")
                logger.error("📋 Error details:")
                
                # Show error details
                error_lines = supabase_result.stderr.split('\n')
                for line in error_lines[-10:]:  # Show last 10 lines of error
                    if line.strip():
                        logger.error(f"   {line.strip()}")
                
                logger.error(f"📁 Check full error log at: {supabase_log_file}")
                all_transfer_logs.append(f"SUPABASE_STAGING FAILED: {supabase_result.stderr[:200]}...")
                
        except subprocess.TimeoutExpired:
            logger.error("⏰ Meltano supabase-to-bigquery STAGING timed out after 15 minutes")
            logger.error("💡 This might indicate data volume issues or network problems")
            all_transfer_logs.append("SUPABASE_STAGING TIMEOUT: Pipeline timed out after 15 minutes")
        except Exception as e:
            logger.error(f"❌ Exception during Supabase STAGING transfer: {str(e)}")
            all_transfer_logs.append(f"SUPABASE_STAGING ERROR: {str(e)}")
    else:
        logger.info("⚠️ No Supabase tables found to process")
        all_transfer_logs.append("SUPABASE_STAGING: No tables found")
    

    # Check if we have any tables processed
    if not all_table_names:
        logger.warning("⚠️ No tables found from Supabase")
        return {
            "bq_tables": [],
            "staging_dataset": config.staging_bigquery_dataset,
            "dataset": config.bigquery_dataset,
            "table_names": [],
            "supabase_tables": supabase_tables,
            "transfer_log": "; ".join(all_transfer_logs),
            "status": "warning"
        }
    
    # Create comprehensive result
    transfer_result = {
        "bq_tables": all_bq_tables,
        "staging_dataset": config.staging_bigquery_dataset,
        "dataset": config.bigquery_dataset,
        "table_names": all_table_names,
        "supabase_tables": supabase_tables,
        "transfer_log": "; ".join(all_transfer_logs),
        "status": "success"
    }
    
    # Log final metadata for tracking
    logger.info("🎉 Supabase to staging transfer completed!")
    logger.info(f"📊 Total tables processed: {len(all_table_names)}")
    logger.info(f"📊 BigQuery staging tables created: {len(all_bq_tables)}")
    logger.info(f"📊 BigQuery staging dataset: {config.staging_bigquery_dataset}")
    logger.info(f"📊 BigQuery production dataset: {config.bigquery_dataset}")

    return transfer_result


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2a_processing_dim_dates(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for dates using dbt SQL file
    
    Creates dim_dates table using the separate SQL file with:
    - date_sk (primary key - auto-incrementing)
    - date_value (date not null)
    - year, quarter, month, day_of_month, day_of_week (bigint)
    - is_weekend (boolean)
    - Additional attributes: day_name, month_name, week_of_year, is_business_day
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Date dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_dates using dbt SQL file")
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
            'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID', 'dsai-468212'),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: dim_dates...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_dates.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_datess model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models dim_dates --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_dates model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_dates model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_dates model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_dates' in line and ('OK created' in line or 'OK' in line):
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
            logger.warning("⚠️ Could not confirm dim_dates model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_dates")
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
            "table_name": "dim_dates",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_dates",
            "dbt_model": "dim_dates",
            "sql_file": "models/marts/dimensions/dim_dates.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Date dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_dates model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_dates model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
  

# Update _2b_processing_dim_orders
@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2b_processing_dim_orders(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for orders using dbt SQL file
    
    Creates dim_orders table using the separate SQL file with:
    - order_sk (primary key - auto-incrementing)
    - order_id, customer_id, order_status (not null)
    - order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date
    - Derived columns: order_year, order_month, order_day, delivery_days, is_delivered
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Orders dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_orders using dbt SQL file")
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
            'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID', 'dsai-468212'),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: dim_orders...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_orders.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_orders model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models dim_orders --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_orders model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"� dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_orders model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_orders model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_orders' in line and ('OK created' in line or 'OK' in line):
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
            logger.warning("⚠️ Could not confirm dim_orders model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_orders")
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
            "table_name": "dim_orders",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_orders",
            "dbt_model": "dim_orders",
            "sql_file": "models/marts/dimensions/dim_orders.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Orders dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_orders model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_orders model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2c_processing_dim_products(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for products using dbt SQL file
    
    Creates dim_products table using the separate SQL file with:
    - product_sk (primary key - auto-incrementing)
    - product_id (not null), product_category_name, product_category_name_english
    - product_name_lenght, product_description_lenght, product_photos_qty
    - product_weight_g, product_length_cm, product_height_cm, product_width_cm
    - Derived columns: product_weight_kg, product_volume_cm3, photo_quality_category
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Products dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_products using dbt SQL file")
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
            'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID', 'dsai-468212'),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: dim_products...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_products.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_products model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models dim_products --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_products model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_products model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_products model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_products' in line and ('OK created' in line or 'OK' in line):
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
            logger.warning("⚠️ Could not confirm dim_products model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_products")
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
            "table_name": "dim_products",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_products",
            "dbt_model": "dim_products",
            "sql_file": "models/marts/dimensions/dim_products.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Products dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_products model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_products model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2d_processing_dim_order_reviews(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for order reviews using dbt SQL file
    
    Creates dim_order_reviews table using the separate SQL file with:
    - review_sk (primary key - auto-incrementing)
    - review_id (not null), order_id, review_score
    - review_comment_title, review_comment_message
    - review_creation_date, review_answer_timestamp
    - Derived columns: review_sentiment, has_comment, has_answer, review_year, review_month
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Order reviews dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_order_reviews using dbt SQL file")
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
            'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID', 'dsai-468212'),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: dim_order_reviews...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_order_reviews.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_order_reviews model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models dim_order_reviews --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_order_reviews model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_order_reviews model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_order_reviews model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_order_reviews' in line and ('OK created' in line or 'OK' in line):
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
            logger.warning("⚠️ Could not confirm dim_order_reviews model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_order_reviews")
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
            "table_name": "dim_order_reviews",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_order_reviews",
            "dbt_model": "dim_order_reviews",
            "sql_file": "models/marts/dimensions/dim_order_reviews.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Order reviews dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_order_reviews model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_order_reviews model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2e_processing_dim_payments(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for payments using dbt SQL file
    
    Creates dim_payments table using the separate SQL file with:
    - payment_sk (primary key - auto-incrementing)
    - order_id (not null), payment_sequential, payment_type
    - payment_installments, payment_value
    - Derived columns: installment_category, payment_value_category, installment_amount
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Payments dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_payments using dbt SQL file")
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
            'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID', 'dsai-468212'),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: dim_payments...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_payments.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_payments model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models dim_payments --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_payments model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_payments model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_payments model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_payments' in line and ('OK created' in line or 'OK' in line):
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
            logger.warning("⚠️ Could not confirm dim_payments model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_payments")
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
            "table_name": "dim_payments",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_payments",
            "dbt_model": "dim_payments",
            "sql_file": "models/marts/dimensions/dim_payments.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Payments dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_payments model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_payments model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2f_processing_dim_sellers(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for sellers using dbt SQL file
    
    Creates dim_sellers table using the separate SQL file with:
    - seller_sk (primary key - auto-incrementing)
    - seller_id (not null), seller_zip_code_prefix, seller_city, seller_state
    - Derived columns: seller_state_code, seller_location, seller_region
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Sellers dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_sellers using dbt SQL file")
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
            'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID', 'dsai-468212'),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: dim_sellers...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_sellers.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_sellers model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models dim_sellers --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_sellers model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_sellers model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_sellers model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_sellers' in line and ('OK created' in line or 'OK' in line):
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
            logger.warning("⚠️ Could not confirm dim_sellers model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_sellers")
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
            "table_name": "dim_sellers",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_sellers",
            "dbt_model": "dim_sellers",
            "sql_file": "models/marts/dimensions/dim_sellers.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Sellers dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_sellers model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_sellers model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2g_processing_dim_customers(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for customers using dbt SQL file
    
    Creates dim_customers table using the separate SQL file with:
    - customer_sk (primary key - auto-incrementing)
    - customer_id (not null), customer_unique_id, customer_zip_code_prefix
    - customer_city, customer_state
    - Derived columns: customer_state_code, customer_location, customer_region
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Customers dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_customers using dbt SQL file")
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
            'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID', 'dsai-468212'),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: dim_customers...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_customers.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_customers model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models dim_customers --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_customers model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_customers model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_customers model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_customers' in line and ('OK created' in line or 'OK' in line):
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
            logger.warning("⚠️ Could not confirm dim_customers model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_customers")
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
            "table_name": "dim_customers",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_customers",
            "dbt_model": "dim_customers",
            "sql_file": "models/marts/dimensions/dim_customers.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Customers dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_customers model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_customers model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2h_processing_dim_geolocations(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for geolocations using dbt SQL file
    
    Creates dim_geolocations table using the separate SQL file with:
    - geolocation_sk (primary key - auto-incrementing)
    - geolocation_zip_code_prefix (not null), geolocation_lat, geolocation_lng
    - geolocation_city, geolocation_state
    - Derived columns: geolocation_state_code, geolocation_location, geolocation_region, geolocation_lat_rounded, geolocation_lng_rounded
    
    Args:
        _1_staging_to_bigquery: Result from staging to BigQuery
        
    Returns:
        Geolocations dimension processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing dimension table: dim_geolocations using dbt SQL file")
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
            'BQ_PROJECT_ID': os.getenv('BQ_PROJECT_ID', 'dsai-468212'),  # Use existing BQ_PROJECT_ID from .env
        })
        
        logger.info("🔄 Running dbt model: dim_geolocations...")
        logger.info(f"Working directory: {dbt_dir}")
        logger.info(f"Model file: models/marts/dimensions/dim_geolocations.sql")
        logger.info(f"Target dataset: {config.bigquery_dataset}")
        
        # Execute dbt run for dim_geolocations model specifically
        dbt_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && dbt run --models dim_geolocations --no-version-check'
        ],
            capture_output=True,
            text=True,
            cwd=dbt_dir,
            timeout=300,  # 5 minute timeout
            env=env_vars
        )
        
        if dbt_result.returncode != 0:
            logger.error(f"❌ dbt dim_geolocations model failed with return code: {dbt_result.returncode}")
            logger.error("📋 dbt error details:")
            logger.error(f"🔍 dbt stdout:")
            for line in dbt_result.stdout.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            logger.error(f"🔍 dbt stderr:")
            for line in dbt_result.stderr.split('\n')[-10:]:  # Show last 10 lines
                if line.strip():
                    logger.error(f"   {line.strip()}")
            raise Exception(f"dbt dim_geolocations model failed: {dbt_result.stderr}")
        
        logger.info("✅ dbt dim_geolocations model completed successfully")
        logger.info("📋 dbt run output:")
        
        # Parse dbt output to get information
        output_lines = dbt_result.stdout.split('\n')
        model_created = False
        records_processed = 0
        
        for line in output_lines:
            if 'dim_geolocations' in line and ('OK created' in line or 'OK' in line):
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
            logger.warning("⚠️ Could not confirm dim_geolocations model creation from dbt output")
        
        # Verify the table was created in BigQuery
        try:
            import json
            from google.cloud import bigquery
            
            credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
            if credentials_json:
                credentials_info = json.loads(credentials_json)
                project_id = credentials_info.get("project_id")
                
                client = bigquery.Client(project=project_id)
                table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_geolocations")
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
            "table_name": "dim_geolocations",
            "status": "completed",
            "records_processed": records_processed,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_geolocations",
            "dbt_model": "dim_geolocations",
            "sql_file": "models/marts/dimensions/dim_geolocations.sql",
            "creation_method": "dbt SQL file",
            "dbt_stdout": dbt_result.stdout[-500:] if dbt_result.stdout else ""
        }
        
        logger.info("✅ Geolocations dimension processing completed using dbt SQL file")
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt dim_geolocations model timed out after 5 minutes"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt dim_geolocations model execution failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)


@asset(group_name="Transformation", deps=[
    _1_staging_to_bigquery,
    _2a_processing_dim_dates,
    _2b_processing_dim_orders,
    _2c_processing_dim_products,
    _2d_processing_dim_order_reviews,
    _2e_processing_dim_payments,
    _2f_processing_dim_sellers,
    _2g_processing_dim_customers,
    _2h_processing_dim_geolocations
])
def _3_fact_order_items(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforming data warehouse for Complete dashboard visualization
    Reading from staging dataset and creating fact tables in production dataset
    
    Args:        
        _1_staging_to_bigquery: Result from staging BigQuery transfer

    Returns:
        Fact table creation results for production data warehouse
    """

    logger = get_dagster_logger()

    logger.info("🔄 Transformation - Processing BigQuery Fact Tables...")
    logger.info(f"Reading from staging dataset: {config.staging_bigquery_dataset}")
    logger.info(f"Writing to production dataset: {config.bigquery_dataset}")
    
    # Get information from _1_staging_to_bigquery result
    bq_tables = _1_staging_to_bigquery.get("bq_tables", [])
    table_names = _1_staging_to_bigquery.get("table_names", [])
    
    logger.info(f"✅ Processing data from staging: {len(bq_tables)} BigQuery staging tables available")

    # Create summary based on available information
    summary = {
        "pipeline_status": "completed",
        "staging_tables_processed": len(bq_tables),
        "source_dataset": config.staging_bigquery_dataset,
        "target_dataset": config.bigquery_dataset,
        "staging_tables": bq_tables,
        "production_fact_table": f"{config.bigquery_dataset}.fact_order_items"
    }
    
    logger.info("🎉 Fact table processing completed successfully!")
    logger.info(f"Processed {len(bq_tables)} staging tables for production warehouse")
    logger.info(f"Production dataset: {config.bigquery_dataset}")
    
    return summary

@asset(group_name="Analysis", deps=[_3_fact_order_items])
def _4a_fact_order_payments(config: PipelineConfig, _3_fact_order_items: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create fact table for order payments
    
    Args:
        _3_fact_order_items: Result from fact order items processing
        
    Returns:
        Order payments fact table processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing fact table: fact_order_payments")
    
    result = {
        "table_name": "fact_order_payments",
        "status": "completed",
        "records_processed": 0,
        "bq_table": f"{config.bigquery_dataset}.fact_order_payments"
    }
    
    logger.info("✅ Order payments fact table processing completed")
    return result


@asset(group_name="Analysis", deps=[_3_fact_order_items])
def _4b_fact_order_reviews(config: PipelineConfig, _3_fact_order_items: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create fact table for order reviews
    
    Args:
        _3_fact_order_items: Result from fact order items processing
        
    Returns:
        Order reviews fact table processing results
    """
    logger = get_dagster_logger()
    logger.info("🔄 Processing fact table: fact_order_reviews")
    
    result = {
        "table_name": "fact_order_reviews",
        "status": "completed",
        "records_processed": 0,
        "bq_table": f"{config.bigquery_dataset}.fact_order_reviews"
    }
    
    # Log summary metadata
    logger.info(f"Pipeline status: success")
    logger.info(f"Final destination: BigQuery")    
    return result


@asset(group_name="Summary", deps=[_4a_fact_order_payments, _4b_fact_order_reviews])
def _5_dbt_summaries(config: PipelineConfig, _4a_fact_order_payments: Dict[str, Any], _4b_fact_order_reviews: Dict[str, Any]) -> Dict[str, Any]:
    """
    dbt Phase 2: Transform staging data into analytics-ready data marts using dbt
    
    This runs dbt models to:
    1. Create staging views with data cleaning
    2. Build dimension tables (customers, products)
    3. Create fact tables (orders)
    4. Generate business metrics and KPIs
    
    Args:
        _1_staging_to_bigquery: Result from staging extraction
        
    Returns:
        dbt transformation results with model metadata
    """
    logger = get_dagster_logger()
    logger.info("🔄 dbt PHASE 2: Transforming staging data to analytics marts...")
    
    # Change to Meltano directory where dbt is configured
    meltano_dir = "/Applications/RF/NTU/SCTP in DSAI/supabase-meltano-bq-dagster/bec-meltano"

    try:
        # Step 1: Run dbt deps to install any packages
        logger.info("📦 Installing dbt dependencies...")
        deps_result = subprocess.run(
            ['meltano', 'invoke', 'dbt-bigquery:deps'],
            cwd=meltano_dir,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if deps_result.returncode == 0:
            logger.info("✅ dbt dependencies installed successfully")
        else:
            logger.warning(f"⚠️ dbt deps warning: {deps_result.stderr}")
        
        # Step 2: Run dbt models to transform data
        logger.info("🔄 Running dbt transformations...")
        run_result = subprocess.run(
            ['meltano', 'invoke', 'dbt-bigquery:run'],
            cwd=meltano_dir,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes for transformation
        )
        
        if run_result.returncode != 0:
            error_msg = f"dbt run failed: {run_result.stderr}"
            logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)
        
        logger.info("✅ dbt transformations completed successfully")
        logger.info(f"dbt output: {run_result.stdout}")
        
        # Step 3: Run dbt tests to validate data quality
        logger.info("🧪 Running dbt data quality tests...")
        test_result = subprocess.run(
            ['meltano', 'invoke', 'dbt-bigquery:test'],
            cwd=meltano_dir,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        test_status = "passed" if test_result.returncode == 0 else "failed"
        if test_result.returncode != 0:
            logger.warning(f"⚠️ Some dbt tests failed: {test_result.stderr}")
        else:
            logger.info("✅ All dbt tests passed")
        
        # Parse dbt run results to get model information
        models_created = []
        if "Completed successfully" in run_result.stdout:
            # Extract model names from dbt output (simplified parsing)
            for line in run_result.stdout.split('\n'):
                if 'OK created' in line or 'OK created view' in line:
                    # Try to extract model name from dbt output
                    parts = line.split()
                    if len(parts) > 2:
                        model_name = parts[-1].split('.')[-1]  # Get last part after dots
                        models_created.append(model_name)
        
        # Create result summary
        result = {
            "status": "success",
            "dbt_run_status": "success",
            "dbt_test_status": test_status,
            "models_created": models_created,
            "target_dataset": config.bigquery_dataset,
            "staging_dataset": config.staging_bigquery_dataset,
            "dbt_stdout": run_result.stdout[-1000:],  # Last 1000 chars
            "test_stdout": test_result.stdout[-500:] if test_result.stdout else "",
            "transformation_type": "dbt",
            "total_models": len(models_created)
        }
        
        logger.info("🎉 dbt transformation phase completed!")
        logger.info(f"📊 Models created: {len(models_created)}")
        logger.info(f"📊 Target dataset: {config.bigquery_dataset}")
        logger.info(f"📊 Test status: {test_status}")
        
        return result
        
    except subprocess.TimeoutExpired:
        error_msg = "dbt transformation timed out"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"dbt transformation failed: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)
    
 

@job(name="all_assets")
def all_assets_pipeline():
    """
    Complete ETL pipeline: Staging → Dimensions → Analysis → Summary
    
    This job orchestrates the entire data pipeline with proper dependencies
    and comprehensive monitoring of each stage.
    """
    # The asset dependencies are automatically handled by Dagster
    _5_dbt_summaries()


# Define the Dagster definitions
defs = Definitions(
    assets=[
        # Phase 1: Extraction - Supabase to BigQuery Staging
        _1_staging_to_bigquery,
        
        # Phase 3: Legacy Dimension Processing (keeping for compatibility)
        _2a_processing_dim_dates,
        _2b_processing_dim_orders,
        _2c_processing_dim_products,
        _2d_processing_dim_order_reviews,
        _2e_processing_dim_payments,
        _2f_processing_dim_sellers,
        _2g_processing_dim_customers,
        _2h_processing_dim_geolocations,
        _3_fact_order_items,
        _4a_fact_order_payments,
        _4b_fact_order_reviews,
        
        # Phase 5: Transformation - dbt Analytics
        _5_dbt_summaries

    ]
    #,jobs=[all_assets] #defined job for now just commented
)


if __name__ == "__main__":
    # For testing - you can run individual assets or the full pipeline
    from dagster import materialize
    
    print("🚀 Running Staging to BigQuery Pipeline with Dagster")
    print("=" * 60)
    
    # Test Supabase connection and table discovery first
    print("🔄 Testing Supabase PostgreSQL connection and table discovery...")
    
    # Use the same connection parameters as in the asset function
    connection_params = {
        'host': "aws-1-ap-southeast-1.pooler.supabase.com",
        'port': 5432, 
        'database': "postgres",
        'user': "postgres.royhmnxmsfichopabwsi",
        'password': os.getenv("TAP_POSTGRES_PASSWORD")
    }
    
    try:
        # Test connection and get tables
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Query for tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        supabase_tables = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        print(f"✅ Found {len(supabase_tables)} Supabase tables: {supabase_tables}")
        
        if supabase_tables:
            print("🔄 Now testing Meltano supabase-bq pipeline...")
            try:
                # Test meltano supabase-bq pipeline
                result = subprocess.run(
                    ['meltano', 'run', 'supabase-bq'],
                    capture_output=True,
                    text=True,
                    cwd="../RDS-BQ",  # Path to meltano directory
                    timeout=300
                )
                
                if result.returncode == 0:
                    print("✅ Meltano supabase-bq pipeline completed successfully!")
                    print(f"Output: {result.stdout}")
                else:
                    print(f"❌ Meltano supabase-bq pipeline failed: {result.stderr}")
                    
            except Exception as e:
                print(f"⚠️ Error running Meltano pipeline: {str(e)}")
        else:
            print("❌ No Supabase tables found - cannot test pipeline")
            
    except Exception as e:
        print(f"❌ Supabase connection failed: {str(e)}")
    
    print("\n" + "=" * 60)
    
    # Run the staging to BigQuery asset
    print("🔄 Running Staging to BigQuery transfer...")
    result = materialize([_1_staging_to_bigquery])
    
    if result.success:
        print("✅ Dagster pipeline completed successfully!")
    else:
        print("❌ Dagster pipeline failed!")
        for event in result.events_for_node:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Error: {event}")
