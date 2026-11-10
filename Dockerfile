FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    git \
 && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md Makefile ./
COPY vdok/ vdok/
COPY tests/ tests/
COPY bench/ bench/
COPY schemas/ schemas/
COPY manifests/ manifests/
COPY docs/ docs/

RUN pip install --no-cache-dir -e ".[dev]"

CMD ["make", "test"]
