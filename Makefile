# Makefile for vendor-data-onboarding-kit

SIZE ?= 10GB
RUNS ?= 5
SEED ?= 1
FAULTS ?= default
VENDOR ?= sec_fsds
DROP ?=
VENV ?= .venv
export VIRTUAL_ENV := $(abspath $(VENV))
export PATH := $(VENV)/bin:$(PATH)

.PHONY: setup lint typecheck test data synth onboard report bench bench-smoke goldens docker

setup:
	@test -d $(VENV) || uv venv $(VENV)
	uv pip install --exact -e ".[dev]"

lint:
	ruff check .
	ruff format --check .

typecheck:
	pyright

test:
	pytest --cov=vdok --cov-report=term-missing --cov-fail-under=85

QUARTERS ?=

data:
	vdok data $(if $(QUARTERS),--quarters "$(QUARTERS)",)

synth:
	vdok synth --size-gb $(SIZE) --seed $(SEED) --faults $(FAULTS)

onboard:
	vdok onboard $(VENDOR) $(DROP)

report:
	vdok report $(VENDOR) --drop $(DROP)

bench:
	python bench/run.py --size $(SIZE) --runs $(RUNS) --seed $(SEED)

bench-smoke:
	python bench/run.py --smoke

goldens:
	pytest tests/golden/test_report_goldens.py --update-goldens -v

docker:
	docker build -t vdok .
	docker run --rm vdok
