# wrk — development commands
# Run `just` or `just --list` to see all available recipes.
# Requires: uv, postgres (for integration tests)

default:
    @just --list

# ── Setup ─────────────────────────────────────────────────────────────────────

# Install all dependencies and set up the dev environment
setup:
    uv sync --extra dev
    pnpm install --dir dashboard
    @echo ""
    @echo "Done. To run integration tests, export WRK_TEST_DSN:"
    @echo "  export WRK_TEST_DSN=postgresql://wrk:wrk@localhost/wrk_test"

# ── Code quality ──────────────────────────────────────────────────────────────

# Run the linter
lint:
    uv run ruff check .

# Format code
format:
    uv run ruff format .

# ── Tests ─────────────────────────────────────────────────────────────────────

# Run tests — optionally pass `unit` or `integration`
#   just test
#   just test unit
#   just test integration
test type="":
    uv run pytest {{ if type == "unit" { "tests/unit/" } else if type == "integration" { "tests/integration/" } else { "" } }}

# ── Dashboard ─────────────────────────────────────────────────────────────────

# Run dashboard dev server (proxies API at localhost:8000)
dashboard:
    pnpm --dir dashboard dev

# ── Docs ──────────────────────────────────────────────────────────────────────

# Serve docs locally at http://127.0.0.1:8000
docs:
    uv run mkdocs serve

# Build or deploy docs — pass `build` or `deploy`
#   just build docs
#   just deploy docs
build target:
    {{ if target == "docs" { "uv run mkdocs build" } else { "echo 'Unknown target: " + target + "'" } }}

deploy target:
    {{ if target == "docs" { "uv run mkdocs gh-deploy --force" } else { "echo 'Unknown target: " + target + "'" } }}

# ── Housekeeping ──────────────────────────────────────────────────────────────

# Remove caches and build artifacts
clean:
    find . -type d -name __pycache__ | xargs rm -r 2>/dev/null; true
    find . -type d -name .pytest_cache | xargs rm -r 2>/dev/null; true
    find . -type d -name .mypy_cache | xargs rm -r 2>/dev/null; true
    find . -name "*.pyc" -delete
    find . -name ".coverage" -delete
