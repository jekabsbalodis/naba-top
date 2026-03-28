# Naba top

Streamlit application for displaying Radio NABA weekly Top&nbsp;10 and Top&nbsp;25 music charts
and Prefect workflow for data scraping and processing.

## Features

- View current week's Top&nbsp;10 and Top&nbsp;25 music charts
- View previous week's music charts
- Automated data scraping from Radio NABA website
- Data processing using DuckDB (views to display data)
- Upload data to an S3 storage

## Prerequisites

- Python 3.14+
- Git or Jujutsu
- Prefect server for workflow orchestration
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
3. Configure Prefect and Streamlit (instructions in configuration section)

## Database setup

Before running any flows, You need to initialize the DuckDB database on the machine You are running the Prefect server.

```bash
uv run -m database.init_db <path/to/database.db>
```

This creates a DuckDB database with the necessary schema. The path must match the `db_path` You will set as a
Prefect variable.

## Configuration

Using the Prefect server's webpage, set Prefect Secret blocks and Variables (or use `uv run prefect variable`
and `uv run prefect block` commands).

You need to set these variables:

1. `db_path` - path of the database to use for the flows, it's the path on the machine You are running the Prefect
   server
2. `flow_url` - the url of the web page to scrape

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

### Running Prefect flow

The project includes the main flow that:

1. Fetches and parses the webpage
2. Hands off the parsed data to two different subflows:
   - Flow that updates the data about songs
   - Flow that updates the data about current charts
3. Uploads the data to an S3 storage.

Deploy the flow with `uv run prefect deploy` and follow the options displayed in the terminal.

You can run the deployment from the Prefect server's webpage and also set the schedule there.

## Project structure

Key files and directories:

- app/ - Streamlit application code
- flows/ - Prefect flows and tasks
  - main.py - Main orchestration flow
  - update_songs.py - Song data processing
  - update_charts.py - Chart data processing
- models.py - Data models (Song, ChartEntry and similar)
- prefect.yaml - Prefect deployment configuration
- pyproject.toml - Project dependencies and configuration

## License

MIT License - see LICENSE for details.
