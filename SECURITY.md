# Security Policy

## Supported Versions

Security fixes are currently considered for the latest `main` branch and the latest published crate/package version.

## Reporting Vulnerabilities

Please do not open public issues for suspected vulnerabilities. Report security concerns privately to the repository maintainers. If GitHub private vulnerability reporting is enabled for this repository, use that path. Otherwise contact the maintainers listed on the repository.

Include the affected version or commit, reproduction steps, expected impact, and whether raw data, credentials, or private datasets are involved.

## Scope Notes

Hawk stores compact distributions by default, but privacy depends on configuration and data shape.

- Raw-log retention is opt-in and can store original records. Treat raw logs as sensitive.
- HTTP ingest is intended for local or trusted deployments unless authentication, authorization, rate limits, TLS, and input hardening are added.
- Distribution outputs can still leak information for small sample sizes or high-cardinality dimensions.
- MCP and Python integrations inherit the filesystem permissions of the process that runs them.
