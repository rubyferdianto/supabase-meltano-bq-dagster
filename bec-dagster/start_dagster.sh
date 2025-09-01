#!/bin/bash

# Full Dagster + Meltano ELT Pipeline Launcher
echo "🚀 Starting Full Dagster + Meltano ELT Pipeline"
echo "========================================================="
echo "📋 Method: TRUNCATE + INSERT loading with dimensional processing"
echo "🎯 Focus: Full ELT with Meltano + complete asset graph"
echo "🌐 Web UI: http://localhost:3000"
echo ""

# Check if complex pipeline file exists
if [ ! -f "dagster_pipeline.py" ]; then
    echo "❌ Error: dagster_pipeline.py not found!"
    echo "Please ensure you're in the correct directory"
    exit 1
fi

# Initialize conda and activate environment
eval "$(conda shell.bash hook)"
conda activate bec

echo "🔧 Environment: bec"
echo "🚀 Launching Dagster server with full asset graph..."
echo ""

# Start Dagster with full complex pipeline
python -m dagster dev -f dagster_pipeline.py --host 127.0.0.1 --port 3000
