# Naba top

Streamlit application for displaying Radio NABA weekly Top&nbsp;10 and Top&nbsp;25 music charts
and pipeline for data scraping and processing (scheduled with [Dagu](https://dagu.sh)).

The app is available at [naba-top.streamlit.app](https://naba-top.streamlit.app).

## Features

- View current week's Top&nbsp;10 and Top&nbsp;25 music charts
- View previous week's music charts
- Automated data scraping from Radio NABA website
- Data processing using DuckDB (views to display data)
- Upload data to an S3 storage

## Prerequisites

- Python 3.14+
- Git or Jujutsu
- Dagu for scheduling, though it's optional because steps are CLI commands
- S3 storage for processed data upload

## Installation

1. Clone the repository (using either `git clone` or `jj git clone`):
   ```bash
   jj git clone https://codeberg.org/clear9550/naba-top.git
   cd naba-top
   ```
2. Install dependencies using `uv`:
   ```bash
   uv sync
   ```
3. Configure the pipeline and Streamlit (instructions in configuration section)

## Database setup

Before running the pipeline, You need to initialize the DuckDB database on the machine You run it.

```bash
uv run -m database.init_db <path/to/database.db>
```

This creates a DuckDB database with the necessary schema. The database must be located in `NABA_TOP_DATA_DIR` and it's
name must match `NABA_TOP_DB_PATH`.

## Configuration

The pipeline reads it's settings from the environment variables (see `.env.example`)

1. `NABA_TOP_DATA_DIR` - directory with the DuckDB file (and `stage/` for runs outside Dagu)
2. `NABA_TOP_DB_PATH` - database file name, relative to `NABA_TOP_DATA_DIR`
3. `NABA_TOP_URL` - the url of the web page to scrape
4. `NABA_TOP_EMAIL` - contact email added to the request header when scraping
5. `S3_KEY_ID`, `S3_SECRET`, `S3_ENDPOINT`, `S3_REGION` - S3 credentials

Put them in a `.env` file, Dagu loads it with `dotenv:`

You also need to set these secret blocks:

1. `flow-email` - this is the email that will be added to request header when scraping `flow_url`
2. `garage-key-id` - Your S3 access key
3. `garage-secret` - Your secret key
4. `garage-endpoint` - Your S3 endpoint url
5. `garage-region` - Your S3 region

Create a .streamlit/secrets.toml file with S3 credentials. This is required, if the app is hosted on Streamlit
community cloud - the Prefect server and Streamlit can't use the same DuckDB database.

```
[S3 endpoint name]
key_id = "your-access-key"
secret = "your-secret-key"
endpoint = "your-s3-endpoint-url"
region = "your-s3-region"
```

## Usage

### Running the Streamlit app

`uv run streamlit run` <- if the file is named `streamlit_app.py` the file name can be omitted

The app will be available at http://localhost:8501

### Running the pipeline

The pipeline is a chain of steps. Each is a CLI command and it passes it's result to the next one through a file saved
to disk.

The files are saved in one of these directories:

- if executed with Dagu, files are saved in artifact directory (`DAG_RUN_ARTIFACTS_DIR`,
  enabled by `artifacts.enabled: true` in the DAG). Files can be browsed and downloaded in from
  the Web UI's Artifacts tab.
- if executed manualy, files are saved in `$NABA_TOP_DATA_DIR/stage/<run id>/`. `<run id>` is `latest` for single steps
  and a timestamp for `run-all` executions. Set `NABA_TOP_RUN_ID` to define a specific folder
  in `$NABA_TOP_DATA_DIR/stage/`

| Step              | Reads                            | Writes                               |
| ----------------- | -------------------------------- | ------------------------------------ |
| `fetch`           | web page                         | `page.html`                          |
| `parse-songs`     | `page.html`                      | `songs.parquet`                      |
| `load-songs`      | `songs.parquet`                  | database                             |
| `parse-charts`    | `page.html`, database (song ids) | `charts.parquet`                     |
| `validate-charts` | `charts.parquet`                 | - (fails if the page layout changed) |
| `load-charts`     | `charts.parquet`                 | database                             |
| `upload`          | database                         | parquet files in S3                  |

Run one step with `uv run python -m pipeline <step>`
and all the steps in one process with `uv run python -m pipeline run-all`.

### Scheduling with Dagu

`dags/naba-top.yaml` defines one Dagu step per pipeline step with retries, max active runs and an optional ntfy
notification on failure.

## Project structure

Key files and directories:

- app/ - Streamlit application code
- pipeline/ - Pipeline steps
  - `__main__.py` - CLI commands, one command per step
  - config.py - Settings from environment variables
  - shared_tasks.py - Steps required for both song and chart parsing (fetching page, uploading to S3)
  - update_songs.py - Song data processing
  - update_charts.py - Chart data processing
- dags/ - Dagu DAG definition
- models.py - Data models (Song, ChartEntry and similar)
- prefect.yaml - Prefect deployment configuration
- pyproject.toml - Project dependencies and configuration

## License

MIT License - see LICENSE for details.
