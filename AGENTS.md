# AGENTS.md

## Purpose

This repository should remain production-grade, maintainable, and reproducible.

All coding agents must optimize for:

- maintainability
- modularity
- reproducibility
- testability
- documentation quality
- scientific/technical rigor (when applicable)
- future extensibility

The codebase must be understandable by another engineer without tribal knowledge.

---

# Core Engineering Rules

## Architecture

Agents must preserve clear modular separation and explicit interfaces.

Recommended top-level structure (adapt as needed per project type):

```text
project/
|-- src/ (or domain modules)
|-- tests/
|-- docs/
|-- scripts/ (optional)
|-- notebooks/ (optional, exploratory only)
|-- README.md
|-- AGENTS.md
`-- (REPORT.md optional for research-heavy projects)
```

Rules:

- Keep modules isolated and cohesive.
- Avoid monolithic scripts for core logic.
- Move reusable notebook logic into versioned modules.
- Prefer composable designs and separation of concerns.
- Prioritize long-term maintainability over short-term convenience.

## Design Patterns Policy

Use Python design patterns pragmatically when they reduce duplication, improve clarity, or make behavior safer to extend.

Preferred usage:

- Strategy pattern for interchangeable behaviors (provider adapters, fetch policies, serialization policies).
- Template Method for shared orchestration with small, well-defined variant steps.
- Factory pattern for constructing typed clients/services without leaking wiring details.
- Repository/DAO style boundaries for storage access to avoid persistence logic in domain workflows.

Rules:

- Do not introduce patterns as ceremony; justify them with concrete simplification.
- Keep pattern boundaries explicit and discoverable in module structure and naming.
- Prefer small pure helper functions before introducing classes.
- Refactors that introduce patterns must preserve behavior and include regression tests.

## Scalability And Reliability Policy

All technical decisions must account for likely growth, such as:

- more data/entities/users/traffic
- larger history/backfills
- higher job frequency
- additional integrations/providers

Required implications:

- Prefer incremental/delta processing over full rescans when feasible.
- Keep operations idempotent.
- Use bounded, configurable concurrency.
- Keep schema changes backward compatible and versioned.
- Preserve observability (progress, throughput, error isolation).
- Use storage/index strategies that remain efficient as volume grows.

---

# Code Quality Rules

## Type Safety

- Use type hints consistently.
- Functions should have explicit return types.

## Documentation

- Non-trivial functions/modules require docstrings.
- Public interfaces should include concise usage guidance.

## Formatting And Static Checks

Code must remain compatible with project quality gates, typically:

- `ruff` (or equivalent linter/formatter)
- `mypy`/`pyright` (or equivalent type checker)
- `pytest` (or equivalent test runner)

## Pre-Commit Quality Gates (MANDATORY)

All changes must satisfy the repository pre-commit hooks. Treat these as mandatory quality gates.

Required commands (as configured in `.pre-commit-config.yaml`):

- `uv run ruff check .`
- `uv run ruff format --check .`
- `uv run mypy .`
- `uv run ty check .`
- `uv run lint-imports --config .importlinter`
- `uv run python scripts/validate_config_with_pydantic.py --config config.yaml`
- `uv run pytest`

Rules:

- Prefer running the full hook-equivalent command set before finalizing meaningful changes.
- If runtime constraints require a narrower run during iteration, execute the full set before completion whenever practical.
- If any required check cannot be run, explicitly state which command was not executed and why.
- Do not claim completion when mandatory checks are failing.

## Error Handling And Contracts

- Fail fast on invalid inputs and invariant violations.
- Raise specific exceptions; avoid broad `except Exception` without re-raise or structured handling.
- Error messages must include actionable context (which symbol, exchange, date range, dataset).
- Public interfaces must document expected inputs/outputs and failure modes.
- Keep I/O boundaries validated (API payload shape, schema fields, config keys).

## Observability And Logging

- Use structured, context-rich logs for long-running tasks and batch workflows.
- Include progress indicators for backfills (task index, symbol, time range, row count, elapsed time).
- Log warnings for partial/fallback behavior and hard limits (pagination caps, retries, truncation).
- Never log secrets or sensitive values.

## Performance And Resource Efficiency

- Avoid repeated full scans when an incremental strategy is available.
- Keep memory usage bounded for large backfills (chunking/streaming where possible).
- Make expensive operations explicit and configurable (timeouts, concurrency, page sizes).
- Prefer deterministic deduplication and stable ordering in outputs.

---

# Testing Rules

After meaningful code changes, run relevant checks and tests.

Minimum expectation:

- run targeted tests for changed areas
- run full test suite before finalization when practical

If checks cannot be run, explicitly state what was not run and why.

Additional expectations:

- Add regression tests for every bug fix.
- Test happy path, edge cases, and failure path for changed logic.
- Keep tests deterministic (fixed timestamps/seeds/fixtures; no flaky external dependency assumptions).
- For pipeline logic, validate idempotency and rerun behavior where relevant.

## CLI Command Validation (MANDATORY)

- Every newly added CLI command must work autonomously as a standalone command invocation.
- Every newly added CLI command must have dedicated automated tests that validate its independent execution path and expected behavior.
- CLI command test coverage must be executed whenever a new CLI command is introduced or an existing CLI command is modified.

---

# Git Hygiene Rules

The repository must not track local-only or cache artifacts.

Always ignore and keep untracked (unless repo intentionally requires otherwise):

- `.venv/`
- `.vscode/`
- `__pycache__/`
- `.pytest_cache/`
- `.mypy_cache/`
- `.ruff_cache/`
- `.cache/`
- `.ipynb_checkpoints/`
- `.env`
- `.env.*`

If local-only files are accidentally tracked, remove them from git index while keeping local copies.

## Commit Message Rules

Use Conventional Commits:

```text
type(scope): short summary
```

Allowed `type` values:

- `feat`
- `fix`
- `refactor`
- `test`
- `docs`
- `chore`
- `ci`
- `build`
- `perf`

Rules:

- Use imperative mood.
- Keep summary concise.
- Prefer a meaningful scope.

---

# Security Rules

- Never commit secrets or credentials.
- Use environment variables and local config files.
- Keep sensitive config out of version control.
- Keep required runtime variables documented directly in `config.yaml`.
- Do not place live secret values in docs.

## Configuration Security Policy (MANDATORY)

- `config.yaml` is the single required runtime configuration source for this repository.
- CLI/runtime usage without `config.yaml` is not allowed.
- `.env` must not be used for runtime configuration.
- `config.yaml` is the canonical config file and may be tracked in git.
- `config.yaml` permissions must be restrictive (no permissions for “others”; recommended `chmod 600 config.yaml`).
- Agents must update `config.yaml` structure/docs together with any code changes that add/remove config keys.

---

# Documentation Rules

## README.md

`README.md` should function as a technical entry point and operations guide.

It should cover:

- project overview
- architecture summary
- setup/install
- usage/workflow
- testing and quality checks
- deployment/runtime notes (if applicable)
- known limitations
- future improvements

## REPORT.md (Optional, Research-Heavy Projects)

If the project is research-heavy, maintain a `REPORT.md` with:

- problem statement and methodology
- dataset/inputs
- results/figures/tables
- assumptions and limitations
- reproducibility notes

## Documentation-Code Consistency (MANDATORY)

- Upon essential code changes, compare `README.md` and (if present) `REPORT.md` against current code behavior.
- Fix all inconsistencies in the same change set.

---

# Reproducibility Rules

- Keep configs and execution paths deterministic where feasible.
- Version important artifacts and schemas.
- Preserve seeds and experiment/runtime config for reproducible runs.

---

# Pull Request / Change Rules

For meaningful changes, agents should:

- keep scope focused
- add or update tests
- update relevant docs
- note architectural implications

For non-trivial changes, include:

- rationale and tradeoffs
- rollback or mitigation notes for operational risk
- explicit note of config/schema/doc updates (or confirmation none were required)

---

# Failure Conditions

Agents must not:

- leave undocumented critical behavior changes
- skip validation without disclosure
- introduce unverifiable claims in documentation/reports
- leave stale docs after essential code changes

---

# End Goal

The repository should remain:

- production-grade for engineers
- reproducible for operators/researchers
- understandable for reviewers
- extensible for future contributors and agents
