.PHONY: install test lint run serve clean

# ── Setup ─────────────────────────────────────────────────────────────────────
install:
	pip install -e ".[dev]"

# ── Quality ───────────────────────────────────────────────────────────────────
lint:
	ruff check src/ flows/ tests/

test:
	pytest tests/ -v --tb=short

coverage:
	coverage run -m pytest tests/
	coverage report -m

# ── Pipeline ──────────────────────────────────────────────────────────────────

run:
	python run.py

run-serve:
	python run.py --serve

run-url:
	python run.py --url $(URL)

serve:
	python run.py --serve-only

# ── Prefect UI (optional) ─────────────────────────────────────────────────────
prefect-start:
	prefect server start

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	rm -rf data/raw/* data/processed/*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete

clean-all: clean
	rm -rf .prefect/ dist/ *.egg-info
