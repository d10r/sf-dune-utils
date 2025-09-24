# SuperToken Holders Sync Daemon

Continuously syncs SuperToken holder data from all mainnet Superfluid networks to Dune Analytics.

## Features

- Runs as a daemon with configurable update intervals
- Fetches networks and tokens dynamically from Superfluid metadata
- Gets holder data from Superfluid API
- Creates CSV files and uploads to Dune
- Archives files with timestamps
- Graceful shutdown with signal handling

## Setup

1. Install dependencies using Poetry:
```bash
poetry install
```

2. Set up Dune Analytics API key:
   - Create a `.env` file in the project directory
   - Add your Dune API key:
   ```
   DUNE_API_KEY=your_dune_api_key_here
   ```
   - Get your API key from https://dune.com/settings/api

3. Create data directory:
```bash
mkdir -p data/archive/{csv,json}
```

## Usage

### Environment Variables

- `UPDATE_INTERVAL`: Update interval in seconds (default: 3600 = 1 hour)
- `DEBUG`: Set to any value to process only the first network for testing

### Running the Daemon

Run the daemon using Poetry:
```bash
poetry run python supertoken_holders_sync.py
```

Or activate the Poetry shell and run directly:
```bash
poetry shell
python supertoken_holders_sync.py
```

### Custom Update Interval

Set a custom update interval (e.g., 30 minutes = 1800 seconds):
```bash
UPDATE_INTERVAL=1800 poetry run python supertoken_holders_sync.py
```

### Stopping the Daemon

The daemon can be stopped gracefully using:
- `Ctrl+C` (SIGINT)
- `kill -TERM <pid>` (SIGTERM)

The daemon will complete the current sync cycle before shutting down.

## Output

The script will:
- Create CSV files in the `data/` directory
- Upload CSV files to Dune Analytics with naming convention: `{chain}_{token}_holders`
- Archive all files in `data/archive/{csv,json}/` with timestamps
- Log all activities to `supertoken_sync.log`

## Logging

The script provides detailed logging including:
- Number of SuperTokens found per chain
- Number of holders for each token
- Processing status and errors
- Upload success/failure status
