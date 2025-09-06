# 🚀 Dagster Pipeline Directory

This directory contains all Dagster-related files for the Supabase-Meltano-BigQuery ETL pipeline.

## 📁 Files

- **`dagster_pipeline.py`** - Main Dagster pipeline definition with all assets
- **`workspace.yaml`** - Dagster workspace configuration
- **`README.md`** - This documentation file

## 🎯 How to Run

### Method 1: Dagster Development Server (Recommended)

```bash
cd DAGSTER
python3 -m dagster dev --host 127.0.0.1 --port 3000
```

Then open: http://127.0.0.1:3000

### Method 2: Direct Python Execution

```bash
cd DAGSTER
python3 dagster_pipeline.py
```

## 📊 Pipeline Assets

1. **`_1_supabase_to_bigquery`** - Extraction: Supabase PostgreSQL to BigQuery
2. **`_2_process_datawarehouse`** - Transformation: DBT processing
3. **`_3_bigquery_to_visualization`** - Visualization: BigQuery to Dashboard

## ⚙️ Configuration

- Environment variables are loaded from `../.env` (parent directory)
- Supabase connection uses TAP_POSTGRES_PASSWORD
- BigQuery datasets: olist_data_staging, olist_data_warehouse
- All subprocess calls execute from the parent directory

## 🔧 Dependencies

Make sure you have installed:

```bash
pip install dagster dagster-webserver python-dotenv
```

Or use the conda environment:

```bash
conda env create -f ../requirements-bec.yaml
conda activate bec
```
