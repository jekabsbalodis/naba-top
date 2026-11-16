# Naba top

Streamlit application for displaying Radio NABA weekly Top&nbsp;10 and Top&nbsp;25 music charts
and pipeline for data scraping and processing (scheduled with [Dagu](https://dagu.sh)).

The app is available at [naba-top.streamlit.app](https://naba-top.streamlit.app).

## Features

- View current week's Top&nbsp;10 and Top&nbsp;25 music charts
- View previous week's music charts
- Automated data scraping from Radio NABA website
- Data transformation with SQLMesh on DuckDB (raw -> dim/fct -> mart models)
- Upload transformed data to an S3 storage as parquet files (or local folder in development mode)

## Prerequisites

- Python 3.14+
- uv
- Git or Jujutsu
- Dagu for scheduling, though it's optional because steps are CLI commands
- S3 storage for processed data upload

## Installation

1. Clone the repository (using either `git clone` or `jj git clone`):
   ```bash
   jj git clone https://codeberg.org/clear9550/naba-top.git
   cd naba-top
   ```
2. Install dependencies using `uv` (streamlit app and pipeline have separate dependency groups):
   ```bash
   uv sync --all-groups
   ```
3. Create the config file from the example and fill in the values (see Configuration):
   ```bash
   cp env.example.toml env.toml
   ```

## Database setup

Before running the pipeline, You need to initialize the DuckDB database on the machine You run it.

```bash
uv run -m database.init_db <path/to/database.db>
```

This creates a DuckDB database with the songs and charts tables. The database must be located in `data.data_dir`
and it's name must match `data.raw_data_db` in `env.toml`.

## Configuration

All settings are defined in an `env.toml` in the repository root. Values can be provided as `NABA_TOP_*` environment
variables (nested values are seperated by `__`, e.g. `NABA_TOP_DATA__DATA_DIR`). Dagu and SQLMesh receive them as
environment variables.

Sections:

1. `environment` - `development` or `production`
2. `[garage]` - S3 `endpoint` and `region`
   - `[garage.read]`
   - `[garage.write]`
     - `key_id` and `secret` pairs. The pipeline uploads with the `write` keys,
       the Streamlit app downloads with the `read` keys.
3. `[auth]` - **required by the Streamlit app**, the login on the Top&nbsp;100 page will not work without it:
   - `redirect_uri`
   - `cookie_secret`
   - `client_id`
   - `client_secret`
   - `server_metadata_url`
     - OAuth settings for `st.login`/`st.logout`. `server_metadata_url` must be `https`,
       and `redirect_uri` must be `https` when `environment` is `production`.
4. `[data]`
   - `data_dir` (absolute path with the DuckDB files, `stage/` for runs outside Dagu
     and `upload/` for the local parquet export)
   - `raw_data_db`
   - `sqlmesh_state_db`
   - `sqlmesh_data_db`
5. `[scrape]`
   - `url` of the web page to scrape
   - `email` added to the request header when scraping
6. `[pipeline]`
   - `ntfy_url` for the Dagu failure notifications

The streamlit app needs only part of the config - `environment`, `[garage]`, `[garage.read]`, `[auth]` and in
`development` mode also `[data.data_dir]`. If hosting the app on community cloud, You can copy only those fields from
the `env.toml`. In local development either copy those fields to `.streamlit/secrets.toml` or set in
`.streamlit.config.toml` value for `secrets` the location of the `env.toml`.

For dagu, generate `.env` file from `env.toml` with `uv run -m scripts.make_dotenv`. On the server that's running dagu,
place the file somewhere where dagu can access it like `/etc/naba-top/.env`
(like it's configured in `dags/naba-top.yaml`).

## Usage

### Running the Streamlit app

`uv run streamlit run` <- if the file is named `streamlit_app.py` the file name can be omitted

The app will be available at http://localhost:8501

### Running the pipeline

The pipeline is a chain of steps. Each is a CLI command and it passes it's result to the next one through a file saved
to disk. The transformation step is a SQLMesh command.

The files are saved in one of these directories:

- if executed with Dagu, files are saved in artifact directory (`DAG_RUN_ARTIFACTS_DIR`,
  enabled by `artifacts.enabled: true` in the DAG). Files can be browsed and downloaded from
  the Web UI's Artifacts tab.
- if executed manualy, files are saved in `$NABA_TOP_DATA_DIR/stage/<run id>/`. `<run id>` is `latest` for single steps
  and a timestamp for `run-all` executions. Set `NABA_TOP_RUN_ID` to define a specific folder
  in `$NABA_TOP_DATA_DIR/stage/`

| Step              | Reads                            | Writes                                                    |
| ----------------- | -------------------------------- | --------------------------------------------------------- |
| `fetch`           | web page                         | `page.html`                                               |
| `parse-songs`     | `page.html`                      | `songs.parquet`                                           |
| `load-songs`      | `songs.parquet`                  | database                                                  |
| `parse-charts`    | `page.html`, database (song ids) | `charts.parquet`                                          |
| `validate-charts` | `charts.parquet`                 | - (fails if the page layout changed)                      |
| `load-charts`     | `charts.parquet`                 | raw data database                                         |
| `update-models`   | raw data database                | SQLMesh databases                                         |
| `upload`          | SQLMesh database                 | parquet files and manifest in S3 (or `<data_dir>/upload`) |

Run one step with `uv run python -m pipeline <step>`
and all the steps in one process with `uv run python -m pipeline run-all`.

**IMPORTANT:**
Before running `update-models`, execute `sqlmesh plan` command so the `prod` environment
in SQLMesh database is available.

### Scheduling with Dagu

`dags/naba-top.yaml` defines one Dagu step per pipeline step with retries, max active runs and an optional ntfy
notification on failure.

## Project structure

Key files and directories:

- app/ - Streamlit application code
- streamlit_app.py - Streamlit entry point
- pipeline/ - Pipeline steps
  - `__main__.py` - CLI commands, one command per step
  - config.py - Settings from environment variables
  - shared_tasks.py - Steps required for both song and chart parsing (fetching page, uploading to S3)
  - update_songs.py - Song data processing
  - update_charts.py - Chart data processing
- data_transform - SQLMesh project: models, audits and tests
- database - DuckDB schema for raw data database and S3 connection helper
- dags/ - Dagu DAG definition
- config_models.py - Settings models
- env.example.toml - Config template
- scripts/make_dotenv.py - script to generate .dotenv file from env.toml
- models.py - Data models (Song, ChartEntry and similar)
- pyproject.toml - Project dependencies and configuration

## License

MIT License - see LICENSE for details.
