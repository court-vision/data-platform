# Data Platform Testing Architecture Spec

## Goals
- Ensure data correctness and API reliability before code reaches production.
- Keep production isolated from test side effects.
- Provide a clear path for local development, CI, and cloud staging validation.
- Surface runtime data-quality results in the data-platform dashboard.

## Principles
- Test fast and often on code changes.
- Test realistic behavior in an isolated environment.
- Never run destructive or heavy test workloads against production data.
- Treat data-quality checks as a first-class runtime signal, separate from unit tests.

## Test Layers

### 1) Unit Tests (`pytest -m unit`)
- Scope: pure functions and isolated logic.
- Examples:
  - fantasy points calculation
  - minutes parsing
  - name normalization
  - date-cutoff logic helpers
- Runtime: seconds.
- Trigger: every PR and push.

### 2) API Tests (`pytest -m api`)
- Scope: FastAPI route contracts, auth, response shape, endpoint behavior with mocked dependencies.
- Examples:
  - token auth enforcement
  - dashboard status payload shape
  - trigger endpoint parameter behavior
- Runtime: seconds to low minutes.
- Trigger: every PR and push.

### 3) Integration Tests (`pytest -m integration`)
- Scope: pipeline + DB behavior with real Postgres schema and mocked upstream APIs.
- Examples:
  - idempotent upserts
  - dedup and gate behavior
  - table-level correctness after a run
- Runtime: minutes.
- Trigger: merge to `main`, nightly schedule, optional manual dispatch.

### 4) Data Quality Checks (`pytest -m quality` + runtime quality runner)
- Scope: table correctness assertions against realistic dataset state.
- Examples:
  - freshness
  - uniqueness
  - FK/orphan checks
  - value range checks
  - cross-table reconciliation
- Runtime: minutes depending on dataset.
- Trigger:
  - automated: nightly in staging
  - manual: from dashboard "Run Data Checks"

## When Tests Run

### A. On Pull Request / Push
- Run `unit + api` tests.
- Purpose: fast developer feedback and merge gating.

### B. On Merge to `main`
- Run `unit + api + integration` (and optionally a quality subset).
- Purpose: validate full service behavior before/with staging promotion.

### C. Nightly Schedule (America/Chicago aligned in CI cron policy)
- Run `integration + quality` against staging environment.
- Purpose: catch data drift and upstream API/data changes that code-change tests miss.

### D. Manual Trigger
- Trigger pipeline runs with explicit overrides from dashboard (existing behavior).
- Trigger data-quality checks on demand from dashboard (planned in next phase).

## Environments and Architecture

### Production (Railway Production Environment)
- Runs live `data-platform` service.
- Uses production Railway Postgres.
- Does not run full automated pytest suite.
- Can expose latest quality-check results for visibility.

### Staging (Railway Staging Environment)
- Separate Railway service + separate Postgres database.
- Receives `main` deploy candidate.
- Receives scheduled integration/quality checks.

### CI Runner (GitHub Actions or equivalent)
- Executes test commands.
- Can run:
  - fast suites with local process-only mocks
  - integration suites using disposable Postgres service container
- Publishes artifacts (coverage, junit) and pass/fail status.

## Disposable Docker Postgres Service
- Definition: a temporary Postgres container started for a test run and deleted afterwards.
- Why:
  - deterministic and isolated
  - no pollution of shared DB
  - reproducible failures
- Where it runs:
  - CI: as a service container in the workflow
  - local: optional, if developer chooses to run integration tests locally
- It is not your production Railway Postgres.

## Cloud-Based Testing with Railway
- Yes, cloud-based testing is recommended for staging validation.
- Recommended split:
  - CI service container DB for deterministic integration tests
  - Railway staging DB for smoke + nightly quality checks on deployed app
- This gives both:
  - reproducibility (container DB)
  - environment realism (staging Railway)

## Dashboard Role
- Current:
  - trigger pipelines manually
  - date overrides
  - status and recent jobs visibility
- Next:
  - add data-quality run summary section
  - add "Run Data Checks" action
  - show failed checks with concise messages
- Dashboard should orchestrate safe quality operations, not execute full pytest suite inline.

## Security and Safety
- Keep internal endpoints token-protected.
- Do not run test suites against production DB.
- Keep manual overrides explicit and logged.
- Separate production and staging tokens/credentials.

## Initial Rollout Plan
1. Establish pytest baseline, markers, and core unit/API coverage.
2. Add CI workflow with push/PR and nightly schedule.
3. Add integration fixtures with disposable Postgres.
4. Add runtime data-quality runner and dashboard integration.
5. Add staging nightly quality checks and alerting.
