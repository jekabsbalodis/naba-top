"""Create dotenv file from app config."""

import json
import tomllib
from pathlib import Path

import typer

from config_models import ENV_FILE, ENV_PREFIX, Config


def flatten(data: dict, prefix: str = ENV_PREFIX):
    """Yield environment varible key=value pairs."""
    for key, value in data.items():
        name = f'{prefix}{key}'.upper()
        if isinstance(value, dict):
            yield from flatten(value, f'{name}__')
        else:
            yield f'{name}={json.dumps(value, ensure_ascii=False)}'


def main() -> None:
    """Generate the dotenv file."""
    with open(ENV_FILE, 'rb') as f:
        raw = tomllib.load(f)

    Config.model_validate(raw)

    Path('.env').write_text('\n'.join(flatten(raw)) + '\n')


if __name__ == '__main__':
    typer.run(main)
