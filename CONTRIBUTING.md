# Contributing

This project was built as a technical challenge, but the bar for changes is the same as any
maintained pipeline: it should run from a clean clone, be tested, and be clear a year later.

## Setup

Requires **Python 3.11+**. Java 11+ is needed only for the PySpark path.

```bash
git clone https://github.com/rmonteiro-pereira/neuro_tech.git
cd neuro_tech
python -m venv .venv
# Windows: .venv\Scripts\activate     Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

Verify the install:

```bash
python -c "from iptu_pipeline import config; print(f'Engine: {config.settings.DATA_ENGINE}')"
```

## Running the pipeline

```bash
python main.py
```

The engine (Pandas or PySpark) is selected by `DATA_ENGINE` — see the README for the
Docker/Spark-standalone route.

## Generated artefacts

`data/gold/plots/*.png` **is** versioned so the README renders without a pipeline run.
`data/gold/plots/*.html` is **not** — each interactive Plotly file embeds the whole Plotly
library. Regenerate them with `python main.py`; they are gitignored.

Do not commit anything under `data/raw/`, `data/bronze/`, `data/silver/` or `data/catalog/`,
and nothing over 5 MB.

## Tests and lint

The test suite lives in `tests/` and runs against the **Pandas** engine on synthetic data —
no Spark, Java, or raw data files required:

```bash
uv sync            # or: pip install -e . --group dev
uv run pytest      # 98 tests
uv run ruff check src scripts dags tests main.py
```

Both commands are enforced by CI (`.github/workflows/ci.yml`) and must be green before a PR
is merged. The Spark/Delta/PyDeequ path is **not** covered by CI (it needs a JVM Spark
session with Maven JAR downloads); if you change that path, run it locally via the
Docker route in the README and say so in the PR.

If you are adding behaviour, add tests alongside it. For a pipeline, the cases worth
covering are the failure paths rather than the happy one — malformed rows, missing or
renamed columns, partial writes, and validation actually rejecting bad input rather than
passing it through.

## Pull requests

- Branch from `main`; do not commit to it directly.
- Use [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`,
  `docs:`, `chore:`, `test:`, `refactor:`.
- Explain **why** in the body; the diff already says what.
- One concern per PR.
