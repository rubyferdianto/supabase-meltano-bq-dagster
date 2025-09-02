@asset(group_name="Transformation", deps=[_1_staging_to_bigquery])
def _2a_processing_dim_date(config: PipelineConfig, _1_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and create dimension table for dates using BigQuery SQL
    
    Creates dim_date table with:
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
    logger.info("🔄 Processing dimension table: dim_date using BigQuery SQL")
    logger.info(f"Reading from staging dataset: {config.staging_bigquery_dataset}")
    logger.info(f"Writing to production dataset: {config.bigquery_dataset}")
    
    try:
        # Load environment variables
        from dotenv import load_dotenv
        load_dotenv('../.env')
        
        # Get BigQuery credentials from environment
        import json
        credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if not credentials_json:
            raise Exception("GOOGLE_APPLICATION_CREDENTIALS_JSON not found in environment")
        
        credentials_info = json.loads(credentials_json)
        project_id = credentials_info.get("project_id")
        
        # Create BigQuery client
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
        
        # SQL to create date dimension table (fallback with fixed date range)
        create_dim_date_sql = f"""
        CREATE OR REPLACE TABLE `{project_id}.{config.bigquery_dataset}.dim_date` AS
        WITH date_spine AS (
          SELECT date_val
          FROM UNNEST(GENERATE_DATE_ARRAY('2015-01-01', '2030-12-31', INTERVAL 1 DAY)) AS date_val
        )
        SELECT 
          -- Primary key: date_sk (auto-incrementing number starting from 1)
          ROW_NUMBER() OVER (ORDER BY date_val) AS date_sk,
          
          -- Date value (not null)
          date_val AS date_value,
          
          -- Year
          EXTRACT(YEAR FROM date_val) AS year,
          
          -- Quarter
          EXTRACT(QUARTER FROM date_val) AS quarter,
          
          -- Month
          EXTRACT(MONTH FROM date_val) AS month,
          
          -- Day of month
          EXTRACT(DAY FROM date_val) AS day_of_month,
          
          -- Day of week (1=Sunday, 7=Saturday in BigQuery)
          EXTRACT(DAYOFWEEK FROM date_val) AS day_of_week,
          
          -- Is weekend (Saturday=7, Sunday=1)
          CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_val) IN (1, 7) THEN TRUE 
            ELSE FALSE 
          END AS is_weekend,
          
          -- Additional useful date attributes
          FORMAT_DATE('%A', date_val) AS day_name,
          FORMAT_DATE('%B', date_val) AS month_name,
          EXTRACT(WEEK FROM date_val) AS week_of_year,
          
          -- Business day indicator (Monday=2 to Friday=6)
          CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_val) BETWEEN 2 AND 6 THEN TRUE 
            ELSE FALSE 
          END AS is_business_day
          
        FROM date_spine
        ORDER BY date_val
        """
        
        logger.info("🔄 Creating dim_date table with BigQuery SQL...")
        logger.info(f"Target table: {project_id}.{config.bigquery_dataset}.dim_date")
        
        # Execute the query
        query_job = client.query(create_dim_date_sql)
        query_result = query_job.result()  # Wait for completion
        
        # Get table info for verification
        table_ref = client.get_table(f"{project_id}.{config.bigquery_dataset}.dim_date")
        records_created = table_ref.num_rows
        
        logger.info(f"✅ dim_date table created successfully")
        logger.info(f"📊 Records created: {records_created:,}")
        logger.info(f"📊 Date range: 2015-01-01 to 2030-12-31")
        logger.info(f"📊 Schema: date_sk (PK), date_value, year, quarter, month, day_of_month, day_of_week, is_weekend, day_name, month_name, week_of_year, is_business_day")
        
        # Verify table structure
        schema_fields = [field.name for field in table_ref.schema]
        logger.info(f"📋 Table columns: {', '.join(schema_fields)}")
        
        # Sample a few records to verify
        sample_query = f"""
        SELECT date_sk, date_value, year, quarter, month, day_of_month, day_of_week, 
               is_weekend, day_name, month_name, is_business_day
        FROM `{project_id}.{config.bigquery_dataset}.dim_date`
        ORDER BY date_value
        LIMIT 5
        """
        
        sample_job = client.query(sample_query)
        sample_results = list(sample_job.result())
        
        logger.info("📋 Sample records:")
        for row in sample_results:
            logger.info(f"   {row.date_sk} | {row.date_value} | {row.day_name} | Q{row.quarter} | M{row.month} | DOM{row.day_of_month} | DOW{row.day_of_week} | Weekend:{row.is_weekend} | Business:{row.is_business_day}")
        
        result = {
            "table_name": "dim_date",
            "status": "completed",
            "records_processed": records_created,
            "source_dataset": config.staging_bigquery_dataset,
            "target_dataset": config.bigquery_dataset,
            "bq_table": f"{config.bigquery_dataset}.dim_date",
            "table_schema": schema_fields,
            "primary_key": "date_sk",
            "creation_method": "BigQuery SQL (direct)",
            "date_range": "2015-01-01 to 2030-12-31"
        }
        
        logger.info("✅ Date dimension processing completed successfully")
        return result
        
    except ImportError as e:
        logger.error(f"❌ BigQuery client not available: {e}")
        logger.info("💡 Install with: pip install google-cloud-bigquery")
        raise Exception(f"BigQuery client required: {e}")
        
    except Exception as e:
        logger.error(f"❌ Date dimension creation failed: {str(e)}")
        raise Exception(f"Failed to create dim_date: {str(e)}")
