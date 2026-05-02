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

---

# Testing Rules

After meaningful code changes, run relevant checks and tests.

Minimum expectation:

- run targeted tests for changed areas
- run full test suite before finalization when practical

If checks cannot be run, explicitly state what was not run and why.

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
- Provide a sanitized tracked config template (`sample_config.yaml`) for required variables.
- Do not place live secret values in docs.

## Configuration Security Policy (MANDATORY)

- `config.yaml` is the single required runtime configuration source for this repository.
- CLI/runtime usage without `config.yaml` is not allowed.
- `.env` must not be used for runtime configuration.
- `config.yaml` must remain untracked in git and must not be committed.
- `config.yaml` permissions must be restrictive (no permissions for “others”; recommended `chmod 600 config.yaml`).
- `sample_config.yaml` must be tracked, must not contain sensitive values, and must stay aligned with required `config.yaml` keys.
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
