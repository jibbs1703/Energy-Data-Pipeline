.PHONY: lint test clear-pycache clear-ruff clear-pytest clear

lint:
	python3 -m ruff format .
	python3 -m ruff check .

test:
	python3 -m pytest tests/

clear-pycache:
	find . -type d -name '__pycache__' -exec rm -rf {} +

clear-ruff: clear-pycache
	find . -type d -name '.ruff_cache' -exec rm -rf {} +

clear-pytest: clear-ruff
	find . -type d -name '.pytest_cache' -exec rm -rf {} +

clear: clear-pytest