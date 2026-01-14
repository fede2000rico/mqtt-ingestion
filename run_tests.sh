#!/bin/bash
# Helper script to run tests from the project root

# Ensure we are in the script's directory (project root usually if script is in root)
cd "$(dirname "$0")"

# Set PYTHONPATH to include root and source directories to support direct imports (e.g. 'import models')
export PYTHONPATH=$PYTHONPATH:$(pwd):$(pwd)/ingestion_aggregated:$(pwd)/ingestion

echo "Running tests..."
python3 -m unittest discover tests

# Check result
if [ $? -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed."
fi
