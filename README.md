# Supabase-Meltano-BigQuery Data Pipeline

A comprehensive end-to-end data pipeline that extracts data from Supabase PostgreSQL, loads it to Google BigQuery, and transforms it using dbt for analytics. Built with **Meltano ELT framework** for production-ready deployment and **Dagster** for orchestration.

**Note**: "bec" stands for "brazilian e-commerce" throughout the project naming convention.

## 🎯 Complete Data Flow

```
Supabase PostgreSQL → Google BigQuery → dbt Transformations
        ↓                    ↓              ↓
   � Raw Data         🏭 Staging DB    � Analytics
        │                    │              │
        └── Meltano ELT ──────┴──────────────┘
```

## 📋 Requirements

### Python Version

- **Python 3.11** (Required for Meltano compatibility)
- **Not compatible with Python 3.13+** (dependency conflicts)

### Recommended Setup

- **Production**: Meltano ELT framework (default)
- **Development**: Direct Python approach (fallback)
- **Orchestration**: Dagster for workflow management
- **Transformation**: dbt for data modeling and analytics

## 🗂️ Current Project Structure

```
supabase-meltano-bq-dagster/
├── bec-meltano/                  # 📁 Production Meltano ELT pipeline
│   ├── meltano.yml               # ⚙️ Meltano configuration
│   ├── rds-to-bq-meltano.py      # � Meltano pipeline runner
│   ├── delete-rds-after-load.py  # 🧹 RDS cleanup after transfer
│   ├── meltano-post-hook.py      # 🔗 Post-transfer automation
│   ├── plugins/                  # 🔌 Meltano extractors & loaders
│   └── .meltano/                 # � Meltano state & metadata
├── bec_dbt/                      # 📁 dbt transformation layer
│   ├── dbt_project.yml           # ⚙️ dbt project configuration
│   ├── profiles.yml              # � BigQuery connection profiles
│   ├── models/                   # 📊 dbt models
│   │   ├── staging/              # 🧹 Raw data cleaning
│   │   ├── warehouse/            # 🏭 Dimensional modeling
│   │   └── analytic/             # � One Big Table analytics
│   ├── macros/                   # 🔧 Reusable SQL macros
│   └── README.md                 # 📖 dbt documentation
├── bec-dagster/                  # 🎼 Orchestration framework
│   ├── dagster_pipeline.py       # 🎯 Dagster asset definitions
│   ├── start_dagster.sh          # 🌐 Dagster web UI launcher
│   └── workspace.yaml            # ⚙️ Dagster configuration
├── .env                          # 🔐 Environment configuration (gitignored)
├── .env.example                  # 📋 Environment template
├── requirements-bec.yaml         # 🐍 Conda environment specification (ONLY requirements file)
└── README.md                     # 📖 This file
```

## 🚀 Quick Start - Complete Pipeline

### 1. Environment Setup

**Conda Environment (Recommended & Only Option)**

```bash
# Create conda environment with Python 3.11 and all dependencies
conda env create -f requirements-bec.yaml
conda activate bec

# Verify installation
python --version  # Should show 3.11.x
meltano --version # Should show 3.7.8+
```

### 2. Configure Environment Variables

```bash
# Copy template and edit with your credentials
cp .env.example .env
nano .env  # Add your database and cloud credentials
```

### 3. Run Complete Pipeline

```bash
# Use Dagster for complete orchestration (recommended)
cd bec-dagster/
./start_dagster.sh

# Or run Meltano ELT pipeline directly
cd bec-meltano/
meltano run supabase-to-bigquery-with-transform
```

## 🎼 Orchestration & Execution Options

### Dagster (Recommended for Development & Monitoring)

```bash
# Start Dagster web UI
cd bec-dagster/
./start_dagster.sh

# Access web interface at http://127.0.0.1:3000
```

### Meltano ELT (Production)

```bash
# Production approach with Meltano
cd bec-meltano/

# Extract from Supabase to BigQuery with dbt transformations
meltano run supabase-to-bigquery-with-transform
```

### dbt Transformations (Data Modeling)

```bash
# Run dbt transformations
cd bec_dbt/

# Run all models
dbt run

# Run specific model layers
dbt run --models staging    # Raw data cleaning
dbt run --models warehouse  # Dimensional modeling
dbt run --models analytic   # Analytics aggregations

# Test data quality
dbt test
```

### Direct Python (Development/Testing)

```bash
# Direct approach for quick testing
cd bec-meltano/
# Use Meltano commands directly for testing
meltano run supabase-to-bigquery
```

## ⚙️ Configuration

### Environment Variables (.env)

```bash
# Supabase PostgreSQL Configuration
TAP_POSTGRES_PASSWORD=your_supabase_password
BQ_PROJECT_ID=your-bigquery-project-id
TARGET_STAGING_DATASET=olist_data_staging
TARGET_RAW_DATASET=olist_data_raw

# Google Cloud Configuration
GOOGLE_APPLICATION_CREDENTIALS_JSON='{"type": "service_account", ...}'

# BigQuery Configuration
BQ_PROJECT_ID=your-project-id
TARGET_STAGING_DATASET=olist_data_staging
TARGET_RAW_DATASET=olist_data_raw

# Pipeline Configuration
```

### dbt Configuration

- **Project**: `bec_dbt/`
- **Profile**: `bec_dbt` (defined in `profiles.yml`)
- **Target**: `dev` (BigQuery)
- **Datasets**: `olist_data_staging`, `olist_data_warehouse`

## 🔧 Development Notes

### Python Version Compatibility

- **Python 3.11**: ✅ Fully supported (recommended)
- **Python 3.12**: ⚠️ Limited support (some dependency issues)
- **Python 3.13+**: ❌ Not supported (major dependency conflicts)

### Production vs Development

- **Production**: Use Meltano ELT framework (`bec-meltano/`) for robust, containerized deployment
- **Development**: Use direct Python approach (`bec-meltano/`) for quick testing
- **Orchestration**: Dagster (`bec-dagster/`) provides workflow management and monitoring
- **Transformation**: dbt (`bec_dbt/`) provides data modeling and analytics
- **CI/CD**: Meltano provides better logging, state management, and error handling

### dbt Model Layers

The pipeline includes comprehensive dbt transformations:

- **Staging**: Raw data cleaning, deduplication, and quality flags
- **Warehouse**: Dimensional modeling with facts and dimensions
- **Analytics**: One Big Table (OBT) aggregations for business intelligence

### Troubleshooting

1. **Meltano installation issues**: Ensure Python 3.11 is active
2. **BigQuery authentication**: Check GOOGLE_APPLICATION_CREDENTIALS_JSON format
3. **Supabase connection**: Verify TAP_POSTGRES_PASSWORD and connection details
4. **dbt issues**: Check BigQuery permissions and dataset existence
5. **Pipeline execution**: Check Meltano logs in `bec-meltano/.meltano/logs/`

## 📊 Pipeline Features

- ✅ **Supabase PostgreSQL data extraction with Meltano**
- ✅ **Automated BigQuery loading with multiple target configurations**
- ✅ **Production-ready Meltano ELT framework**
- ✅ **Comprehensive dbt transformations (staging → warehouse → analytics)**
- ✅ **Dagster orchestration with web UI monitoring**
- ✅ **Data quality checks and validation**
- ✅ **Environment-based configuration**
- ✅ **Customer segmentation and analytics macros**
- ✅ **Multiple execution modes (production/development)**

## 🗂️ Key Components

### Meltano ELT Pipeline

- **`bec-meltano/meltano.yml`**: Meltano configuration with Supabase and BigQuery targets

### dbt Transformations

- **`bec_dbt/models/staging/`**: Raw data cleaning and quality flags
- **`bec_dbt/models/warehouse/`**: Dimensional modeling (dim*\*, fact*\*)
- **`bec_dbt/models/analytic/`**: One Big Table analytics aggregations
- **`bec_dbt/macros/`**: Reusable SQL macros for business logic

### Orchestration

- **`bec-dagster/dagster_pipeline.py`**: Workflow orchestration assets

## 🤝 Contributing

1. Ensure Python 3.11 conda environment: `conda activate bec`
2. Install environment: `conda env create -f requirements-bec.yaml`
3. Test with Meltano: `meltano run supabase-to-bigquery-with-transform`
4. Test dbt transformations: `cd bec_dbt && dbt run && dbt test`
5. Update documentation for any new features

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
