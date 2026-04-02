.PHONY: lint test package install clean run

install:
	pip install -e ".[dev]"

lint:
	black src/ tests/
	flake8 src/ tests/ --max-line-length=88 --extend-ignore=E203,W503

test:
	pytest tests/ -v

package:
	python -m build

run:
	python -m src.main

clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
