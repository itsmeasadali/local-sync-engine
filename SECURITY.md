# Security policy

Please report vulnerabilities through [GitHub's private vulnerability reporting](https://github.com/itsmeasadali/local-sync-engine/security/advisories/new) rather than opening a public issue. Include affected versions, reproduction steps, impact, and any proposed mitigation. Maintainers should acknowledge a report within seven days and coordinate disclosure after a fix is available.

The `main` branch and latest tagged release are supported during the beta. This project does not provide transport encryption: production deployments must terminate TLS before the server. Treat bearer credentials as passwords, use an isolated PostgreSQL role, and never enable bootstrap credentials in a public production environment.

The detailed trust analysis is in [docs/threat-model.md](docs/threat-model.md).
