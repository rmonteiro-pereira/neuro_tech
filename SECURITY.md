# Security Policy

## Reporting a vulnerability

Please report security issues privately rather than opening a public issue.

- Use GitHub's [private vulnerability reporting](https://github.com/rmonteiro-pereira/neuro_tech/security/advisories/new) — preferred.
- Or email **rmonteiropereira1@gmail.com** with `SECURITY` in the subject.

Include what you ran (commit and engine — Pandas or PySpark), the steps to reproduce, and
what you observed. Expect an acknowledgement within **7 days**; this is a personal project,
so treat that as best effort rather than a guarantee.

## Scope

This project is a batch data pipeline over **public** IPTU (municipal property tax) data.
It handles no authentication and stores no credentials. The areas worth reporting:

- **Path handling** — configured input/output paths escaping their intended directory.
- **Deserialisation** — crafted Parquet/Delta input causing unsafe behaviour.
- **Dependency vulnerabilities** reachable through this pipeline's entry points.
- **Accidentally committed secrets** — see below.

## Out of scope

- The accuracy of the underlying municipal dataset.
- Resource exhaustion from deliberately configuring a local Spark job beyond the host's
  capacity.

## Data and secrets

The pipeline reads public open data. **No credentials, tokens or private endpoints belong in
this repository.** `.gitignore` excludes `.env`, the raw/bronze/silver/catalog data layers,
logs and generated artefacts. If you find anything credential-shaped committed here, report
it through the private channel above rather than opening an issue.
