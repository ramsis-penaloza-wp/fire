.PHONY: install lint test run

install:
	uv sync --group dev

lint:
	uv run ruff check .

test:
	uv run pytest

run:
	uv run python -m fire.apps.incident_engine
