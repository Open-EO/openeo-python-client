# Guidance for AI coding agents

This file is for AI coding agents (e.g. Claude Code, GitHub Copilot, Cursor,
Codex, aider, ...) working in this repository. If you are such an agent,
please follow the guidance below.

## Where to find things

- Developer setup, running tests, building docs, and the release procedure
  are documented in [`docs/development.rst`](docs/development.rst) — read
  it before making changes, don't infer these workflows from scratch.
- Code style is enforced through `pre-commit` (see
  [`.pre-commit-config.yaml`](.pre-commit-config.yaml)): `black`-compatible
  formatting via `darker`, `isort` (black profile), line length 120
  (see [`pyproject.toml`](pyproject.toml)). Run `pre-commit run --all-files`
  before proposing a change, if possible.
- Tests live under `tests/` and use `pytest`. Add or update tests for any
  behavior change.
- User-facing and other non-trivial changes should get an entry in
  `CHANGELOG.md` under the `[Unreleased]` section,
  per the existing [Keep a Changelog](https://keepachangelog.com) format used in that file.
  Append new entries at the *bottom* of the relevant subsection
  (e.g. `### Added`), not the top.

## Scope of changes

- Keep feature branches and pull requests focused on a single, well-scoped change.
  Avoid drive-by refactors or unrelated fixes bundled into the same pull request.
- Don't introduce new dependencies or broad API changes without clear
  justification.

## Motivation

Reviewing a pull request takes maintainer time
regardless of who or what wrote it,
so every pull request should be traceable to a genuine need:

- Link the existing GitHub issue(s) it addresses, or, if there isn't one,
  describe the concrete real-world openEO/earth-observation use case or
  bug that motivates the change.
- Don't open pull requests to solve problems invented by scanning the
  codebase for generic "possible improvements" (style nits, speculative
  abstractions, unrequested refactors) without a concrete underlying need.
  This applies whether the pull request is initiated by a human prompting an
  agent or by an agent operating autonomously.
  "My AI agent found something to fix" is not, by itself, sufficient
  motivation.

## Disclosure

This project welcomes AI-assisted contributions, but as maintainers we
want to be able to weigh a pull request appropriately, so please help us do that:

- If you are an AI agent authoring or substantially contributing to a
  commit or pull request, say so in the pull request description
  (e.g. "This PR was drafted with the help of $TOOL_OR_AGENT_NAME").
  A short note is enough, no need for elaborate disclaimers.
- Human contributors using AI assistance (autocomplete, chat-based
  coding agents, etc.) are likewise encouraged to mention it in the pull request
  description when the assistance was substantial.
- Regardless of how a change was produced, the person opening the pull request
  is expected to understand the change, be able to explain the reasoning
  behind it, and respond to review feedback on it.
