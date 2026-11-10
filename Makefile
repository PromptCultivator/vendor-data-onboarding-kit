# Makefile for vendor-data-onboarding-kit

SIZE ?= 10GB
RUNS ?= 5
SEED ?= 1
FAULTS ?= default
VENDOR ?= sec_fsds
DROP ?=
VENV ?= .venv
export PATH := $(VENV)/bin:$(PATH)

.PHONY: setup lint typecheck test data synth onboard report bench bench-smoke goldens docker

setup:
	uv pip install -e ".[dev]"

lint:
	ruff check .
	ruff format --check .

typecheck:
	pyright

test:
	pytest --cov=vdok --cov-report=term-missing --cov-fail-under=85

data:
	@echo "Data acquisition target configured for SEC FSDS range"

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
	@echo "Golden report update stub"

docker:
	docker build -t vdok .
	docker run --rm vdok
