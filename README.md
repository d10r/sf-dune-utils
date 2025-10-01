# Superfluid Dune Sync

Syncs Superfluid data to Dune Analytics.

## Scripts

- **`supertoken_holders_sync.py`** - Syncs SuperToken holder data fetched from supertoken-api from all mainnet networks
- **`sup_metrics_sync.py`** - Syncs SUP distribution metrics fetched from sup-metrics-api daily

## Setup

Install dependencies:
```bash
poetry install
```

Create `.env` file:
```bash
DUNE_API_KEY=your_api_key_here
```

Get your API key from https://dune.com/settings/api

## Usage

Run SuperToken holders sync:
```bash
poetry run python supertoken_holders_sync.py
```

Run SUP metrics sync:
```bash
poetry run python sup_metrics_sync.py
```

## Environment Variables

- `DUNE_API_KEY` - Required for uploads
- `SUPERTOKEN_HOLDERS_UPDATE_INTERVAL` - Update interval in seconds (default: 86400)

## Output

Data is uploaded to Dune namespace `superfluid_hq`:
- SuperToken holders: `{chain}_{token}_holders` tables
- SUP metrics: `sup_metrics_history` table (daily appends)
