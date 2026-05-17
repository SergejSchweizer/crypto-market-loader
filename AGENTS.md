# AGENTS.md

## Purpose

This repository provides a generic, reusable agent instruction baseline for integration into other repositories.

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

## Core Rules (Always Active)

### Minimum Change Principle

- Prefer the smallest safe change that fully resolves the problem.
- Prefer clarity over cleverness.
- Preserve backward compatibility unless a breaking change is intentional and documented.

### Modularity And Interfaces

- Keep module boundaries explicit and cohesive.
- Keep side effects isolated behind adapters/interfaces.
- Keep business logic separate from framework/storage details.

### Reproducibility

- Keep execution paths deterministic where feasible.
- Version important artifacts and schemas.
- Preserve seeds and runtime configuration needed for reproducible runs.

### Documentation

- Keep operational docs aligned with code behavior.
- Do not leave critical behavior changes undocumented.

---

## Architecture

Apply these rules when a task involves system design, module boundaries, refactoring strategy, scalability, reliability, or technical tradeoffs.

### Architecture Goals

- Preserve clear modular separation and explicit interfaces.
- Optimize for maintainability, extensibility, and reproducibility.
- Keep business logic separated from infrastructure and framework details.

### Definition Of Done (Architecture)

- Boundaries and responsibilities are explicit in code structure and naming.
- New/changed contracts are documented and validated at boundaries.
- Scalability and reliability implications are addressed (not deferred implicitly).
- Refactor behavior is covered by regression tests.

### Architecture Rules

- Keep modules isolated and cohesive.
- Avoid monolithic scripts for core logic.
- Move reusable notebook logic into versioned modules.
- Prefer composable designs and separation of concerns.
- Prioritize long-term maintainability over short-term convenience.

### Interface and Contract Practices

- Define contract shape first (types, schema, invariants), then implement.
- Make invalid states unrepresentable with DTOs, enums/literals, and validation.
- Keep backward compatibility by default; version only intentional breaking changes.
- Keep ownership explicit for each module (inputs, outputs, side effects).

### Design Patterns Policy

Use patterns pragmatically only when they reduce duplication, improve clarity, or improve safe extensibility.

Preferred usage:

- Strategy pattern for interchangeable behaviors.
- Template Method for shared orchestration with well-defined variant steps.
- Factory pattern for constructing typed clients/services.
- Repository/DAO boundaries for storage access and persistence isolation.

Rules:

- Do not introduce patterns as ceremony.
- Keep pattern boundaries explicit and discoverable.
- Prefer small pure helper functions before introducing classes.
- Pattern-introducing refactors must preserve behavior and include regression tests.

### Scalability and Reliability Policy

Technical decisions must account for growth in data volume, entities/users/traffic, history size, job frequency, and integrations/providers.

Required implications:

- Prefer incremental/delta processing over full rescans when feasible.
- Keep operations idempotent.
- Use bounded, configurable concurrency.
- Keep schema changes backward compatible and versioned.
- Preserve observability (progress, throughput, error isolation).
- Use storage/index strategies that remain efficient as volume grows.

### Operational Design Practices

- Design workflows to be restart-safe and idempotent by default.
- Bound memory and concurrency with explicit configuration knobs.
- Isolate external dependencies with adapters to support retries, fallback, and test doubles.
- Prefer deterministic ordering and deduplication in persistent outputs.

### Architecture Review Checklist

- Are layering boundaries preserved?
- Does dependency direction flow from policy to implementation?
- Are contracts explicit, typed, and validated?
- Is the change idempotent and restart-safe where required?
- Are tradeoffs, risks, and migration implications documented?

---

## Code Review

Apply these rules when reviewing changes, preparing PRs, or running quality gate validation before merge.

### Review Priorities

- Bugs and behavioral regressions.
- Contract and schema integrity.
- Architectural boundary violations.
- Missing tests for risk-heavy logic.
- Operational risk (idempotency, restartability, observability).

### Severity Model

- High: correctness, data loss/corruption, security, broken contracts, runtime failure.
- Medium: maintainability hazards, missing edge-case handling, observability gaps.
- Low: style/documentation polish, non-blocking improvements.

### Code Quality Rules

- Use type hints consistently, including explicit return types.
- Require docstrings for non-trivial modules/functions and concise usage notes for public interfaces.
- Keep code compatible with explicit quality tooling.

Preferred tooling:

- Linting: `ruff` (or configured equivalent).
- Formatting: `ruff format` (or configured equivalent).
- Type checking: `mypy` or `pyright` (project standard).
- Tests: `pytest` (or configured equivalent).
- Import boundaries: `lint-imports` (or configured equivalent).

Pre-commit quality gates must include lint, format, typing, import-boundary checks, tests, and coverage.

### Review Workflow

1. Understand intended behavior and scope.
2. Validate correctness and contract compatibility first.
3. Check failure paths, error messaging, and observability.
4. Verify tests and coverage for changed risk areas.
5. Check documentation, configuration, and schema alignment.
6. Report findings ordered by severity with actionable guidance.

### Anti-Patterns To Flag

- Silent fallback that hides broken state.
- Broad exception handling without context or re-raise strategy.
- Hidden side effects across module boundaries.
- Untyped public interfaces.
- Contract changes without migration notes.

### PR Guidance

- Keep scope focused.
- Add/update tests.
- Update relevant docs.
- Note architectural implications and rollback/mitigation notes for operational risk.

---

## Testing

Apply these rules when adding/changing tests, fixing bugs, refactoring behavior, adding CLI commands, or validating release readiness.

### Testing Rules

- Run targeted tests for changed areas.
- Run full test suite before finalization when practical.
- Disclose any checks that could not run and why.
- Add regression tests for every bug fix.
- Test happy path, edge cases, and failure paths.
- Keep tests deterministic.

### Test Design Practices

- Prefer behavior-focused tests over implementation-coupled tests.
- Use small, named fixtures with explicit setup intent.
- Cover boundary values, empty inputs, and malformed inputs.
- Validate outcomes and failure modes (error types/messages).

### Coverage Policy (MANDATORY)

- Target repository test coverage is 90%.
- Preserve or improve coverage for meaningful changes.
- Prioritize highest-risk paths first: correctness, persistence, contracts, orchestration, failure handling.
- If measured coverage is below 90%, disclose the gap and required follow-up work.

### Refactoring Validation

For large changes:

1. Split work into small, testable steps.
2. Run targeted tests after each step.
3. Keep behavior stable between steps.
4. Re-run full tests and quality gates at the end.
5. Update docs in the same change set when behavior/process changed.

### CLI Validation

- Every new or modified CLI command must have dedicated automated tests.
- CLI commands must run autonomously as standalone invocations.

---

## Security

Apply these rules when touching configuration, credentials, secrets handling, runtime environment, or sensitive data paths.

### Security Rules

- Never commit secrets or credentials.
- Keep sensitive values out of version control.
- Keep required runtime variables documented in canonical configuration.
- Do not place live secret values in docs.

### Security Engineering Practices

- Apply least privilege for runtime identities and permissions.
- Validate and sanitize external inputs at trust boundaries.
- Prefer explicit allowlists over implicit trust.
- Keep dependency and supply-chain risk visible.
- Treat logs/metrics/traces as potential exfiltration paths.

### Configuration Policy

- Use one canonical runtime configuration source per repository.
- Runtime usage without that canonical source is not allowed.
- Avoid ad-hoc local environment files as runtime source of truth.
- Update config structure and docs in the same change set when keys change.

### Security Checklist

- Are secrets excluded from code/docs/artifacts?
- Are config/runtime contracts explicit and validated?
- Are permissions and access scopes minimized?
- Are errors actionable without leaking sensitive data?
- Are third-party interactions bounded by timeout/retry/input validation?

---

## End Goal

Any repository using these instructions should remain:

- production-grade for engineers
- reproducible for operators/researchers
- understandable for reviewers
- extensible for future contributors and agents
