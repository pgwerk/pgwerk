.DEFAULT_GOAL := help

.PHONY: help setup lint format test test-unit test-integration docs docs-build docs-deploy dashboard clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────────────────────

setup: ## Install all dependencies and set up the dev environment
	uv sync --extra dev
	pnpm install --dir dashboard
	@echo ""
	@echo "Done. To run integration tests, export WRK_TEST_DSN:"
	@echo "  export WRK_TEST_DSN=postgresql://wrk:wrk@localhost/wrk_test"

# ── Code quality ──────────────────────────────────────────────────────────────

lint: ## Run the linter
	uv run ruff check .

format: ## Format code
	uv run ruff format .

# ── Tests ─────────────────────────────────────────────────────────────────────

test: ## Run all tests (requires WRK_TEST_DSN for integration)
	uv run pytest

test-unit: ## Run unit tests only
	uv run pytest tests/unit/

test-integration: ## Run integration tests (requires WRK_TEST_DSN)
	uv run pytest tests/integration/

# ── Dashboard ─────────────────────────────────────────────────────────────────

dashboard: ## Run dashboard dev server (proxies API at localhost:8000)
	pnpm --dir dashboard dev

# ── Docs ──────────────────────────────────────────────────────────────────────

docs: ## Serve docs locally at http://127.0.0.1:8000
	uv run mkdocs serve

docs-build: ## Build docs to site/
	uv run mkdocs build

docs-deploy: ## Deploy docs to GitHub Pages
	uv run mkdocs gh-deploy --force

# ── Housekeeping ──────────────────────────────────────────────────────────────

clean: ## Remove caches and build artifacts
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null; true
	find . -type d -name .pytest_cache -exec rm -r {} + 2>/dev/null; true
	find . -type d -name .mypy_cache  -exec rm -r {} + 2>/dev/null; true
	find . -name "*.pyc" -delete
	find . -name ".coverage" -delete
