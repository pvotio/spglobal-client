# S&P Global ESG Scores Scraper

This project provides a scalable, multi-process scraping pipeline to collect Environmental, Social, and Governance (ESG) scores from S&P Global's publicly available web interface. The scraper collects dimension-specific ESG metrics for thousands of companies and persists the cleaned dataset into a Microsoft SQL Server database for downstream analytics.

## Overview

### Objective

Designed for ESG research analysts and enterprise data teams, this tool automates the ingestion of:
- Overall ESG ratings
- Environmental, Social, and Governance dimension scores
- Industry benchmark data

It supports high-volume parallel execution using multiprocessing and multithreading, with fault tolerance and retry handling built in.

## Data Source

**Base URL:**  
`https://www.spglobal.com/esg/scores/results?cid=<company_id>`

The company IDs are provided via `tickers.json` (bundled in the scraper). For each ID:
- The scraper visits the public ESG profile page
- Extracts scores from embedded HTML attributes
- Maps country names to ISO3 codes using `countries.json`

## Output Fields

For each company, the transformed output includes:

- `name`: Company name  
- `ticker`: Stock ticker  
- `industry`: Industry classification  
- `country`, `country_iso3`: Full name and ISO3 code  
- `score`: Overall ESG score  
- `availability`: Data availability level (e.g., "Available", "Limited")  
- `score_env`, `score_env_ind_average`, `score_env_ind_max`  
- `score_social`, `score_social_ind_average`, `score_social_ind_max`  
- `score_goveco`, `score_goveco_ind_average`, `score_goveco_ind_max`  
- `id`: Original company ID used in the query  
- `url`: Page from which data was scraped  
- `timestamp_created_utc`: Ingestion time

## Architecture

1. **Initialization**  
   Loads all company IDs from `tickers.json`.

2. **Parallel Execution**  
   Launches multiple processes, each managing multiple threads for fetching company ESG pages.

3. **HTML Parsing**  
   Uses `BeautifulSoup` to extract structured data directly from HTML tag attributes.

4. **Transformation**  
   Normalizes nested ESG fields into a flat `DataFrame`.

5. **Database Load**  
   Inserts the final result into a configured Microsoft SQL Server table.

## Project Structure

```
spglobal-client-main/
├── main.py                     # Application entry point
├── scraper/                    # Data fetching logic
│   ├── spglobal.py
│   ├── request.py
│   ├── countries.json
│   ├── tickers.json
│   └── useragents.txt
├── transformer/                # Transformation layer
│   └── agent.py
├── database/                   # MSSQL interface
│   └── mssql.py
├── config/                     # Logging and environment settings
│   ├── logger.py
│   └── settings.py
├── .env.sample                 # Environment variable template
├── Dockerfile                  # Container support
├── requirements.txt            # Python dependencies
```

## Configuration

Copy `.env.sample` to `.env` and set:

| Variable | Description |
|----------|-------------|
| `THREAD_COUNT` | Threads per process |
| `OUTPUT_TABLE` | SQL Server table name |
| `LOG_LEVEL` | `INFO`, `DEBUG`, etc. |
| `INSERTER_MAX_RETRIES` | Retry count for DB operations |
| `REQUEST_MAX_RETRIES`, `REQUEST_BACKOFF_FACTOR` | Backoff config |
| `MSSQL_*` | SQL Server connection credentials |
| `BRIGHTDATA_*` | Optional proxy support for IP rotation |

## Running

### Docker

```bash
docker build -t spglobal-client .
docker run --env-file .env spglobal-client
```

### Local Python

```bash
pip install -r requirements.txt
python main.py
```

## Logging

Tracks scraper progress, transformation events, and MSSQL loading outcomes.

## License

Distributed under the MIT License. Use of S&P Global public content must comply with their content terms of use.
