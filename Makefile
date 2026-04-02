.PHONY: lint test package clean

lint:
	black --check src/ tests/
	flake8 src/ tests/

test:
	pytest tests/ -v

package:
	pip install build && python -m build --wheel --outdir dist/

clean:
	rm -rf dist/ build/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
