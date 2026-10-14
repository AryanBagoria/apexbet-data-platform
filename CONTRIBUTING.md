# Contributing to ApexBet

Thanks for your interest. ApexBet is a **local-first, synthetic-data-only**
demonstration platform. Contributions are welcome, with a few firm rules.

## Ground rules (please respect these)

1. **Never connect to anything real.** No real sportsbook/casino/payment APIs,
   no real AWS account, no external AI API keys. If a change needs those, it
   belongs behind an explicit, documented opt-in and off by default.
2. **Never commit secrets or generated artifacts.** `.env`, `terraform.tfstate`,
   `lake/`, benchmark output and `*.duckdb` are gitignored — keep it that way.
3. **Only synthetic data.** Do not add real customer, account, or transaction
   data of any kind.
4. **Money is integer minor units.** `Decimal` is allowed only at boundaries
   (I/O, display, parsing). Never float.
5. **Enrichment stays advisory.** No enrichment or model output may settle a bet,
   restrict an account, or make a consequential decision. Rules must always
   remain available as a fallback.
6. **Do not silently fall back.** Transport selection etc. must fail fast on an
   unknown value rather than quietly choosing a default.
7. **No fabricated results.** Benchmarks, precision/recall, and coverage numbers
   must be measured and reproducible. If you can't measure it, don't claim it.

## Development setup

```bash
git clone <your-fork>
cd apexbet
make setup            # .venv + project + dev extras
make demo             # confirm the offline pipeline works
```

The project targets **Python 3.11** (system Python 3.9 is too old). `make setup`
prefers `uv` if available, else falls back to `python3 -m venv`.

## Making a change

```bash
git switch -c feat/short-description

# ... edit ...

make format           # ruff format + import sort
make lint             # ruff check  → must pass
make typecheck        # mypy        → must pass
make test             # full suite  → 0 failures (infra tests self-skip)
make check            # lint + typecheck + tests in one go
```

If your change touches SQL marts or transforms:

```bash
make validate-dbt     # compiles + runs every dbt model on local DuckDB
```

If your change touches infrastructure:

```bash
make terraform-validate
```

### Adding a test

Pick the right layer (see [docs/testing](docs/testing/README.md)):

- `tests/unit/` — pure logic, no services.
- `tests/contract/` — shared behaviour every implementation must honour
  (e.g. a new transport must pass the transport contract).
- `tests/reliability/` — failure-mode guarantees (dedup, retry, DLQ).
- `tests/integration/` — mark `@pytest.mark.integration`, self-skip when infra
  is absent. Never make a test *silently pass* when it can't run — skip it.

### Adding an event type

1. Add/update the model in `src/apexbet/schemas/events.py`.
2. Teach the generator to emit it (`src/apexbet/generator/engine.py`).
3. Regenerate schemas: `make schemas`.
4. Add contract coverage in `tests/contract/test_event_contract.py`.

## Commit style

Conventional-ish, imperative subject:

```
feat(transports): add Kafka admin topic preflight
fix(silver): classify late events against watermark, not wall clock
docs(enrichment): document optional enrichment modes
test(reliability): cover DLQ routing on unparseable payload
```

Keep PRs focused. Explain the *why* in the body; link to a decision entry in
[`docs/DECISIONS.md`](docs/DECISIONS.md) when you make a design choice.

## Documentation

- Architecture/data/ops/enrichment/testing docs live under `docs/`. Keep Mermaid
  diagrams in sync with reality.
- Record non-obvious decisions in `docs/DECISIONS.md` as `D-0NN`.
- Update `docs/PROGRESS.md` when a milestone's status changes.

## Reporting issues

Use the GitHub issue tracker. Include OS, Python version, the exact command, the
observed output, and the expected output. Redact anything environment-specific.

## License

By contributing you agree your contributions are licensed under the repository's
MIT license (see [LICENSE](LICENSE)).
