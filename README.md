# MQTT Ingestion

## Project Structure
- `machines_config.json`: Configuration for machines and expected messages.
- `ingestion_aggregated/`: Main service code (Buffered Version).
- `simulator/`: MQTT Simulation.
- `tests/`: Unit tests.

## Running Tests
To run the tests, you need to ensure the project root is in your `PYTHONPATH`.

### Option 1: Use the helper script
```bash
chmod +x run_tests.sh
./run_tests.sh
```

### Option 2: Manual execution
From the project root:
```bash
export PYTHONPATH=.
python3 -m unittest discover tests
```
OR
```bash
export PYTHONPATH=.
python3 tests/ingestion_aggregated/test_manager.py
```

## Running the Project
```bash
docker-compose up --build
```
