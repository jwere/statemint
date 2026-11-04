# StateMint

StateMint turns client data files into finished statements, records and analysis inputs. Business users control mapping, layout and output through an internal web app; developers extend it with new components and rules.

- **Design:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Core logic:** [docs/PSEUDOCODE.md](docs/PSEUDOCODE.md)

## Status

Phase 1 is under way: intake, mapping, rendering and output for one or two pilot clients, then a parallel run against the current process. This repository currently holds the project skeleton, the baseline database schema and the path-safety helper used for every output file name.

## Stack

| Part | Version |
| --- | --- |
| Java (Eclipse Temurin) | 25 LTS |
| Spring Boot | 4.1 |
| PostgreSQL | 18 |
| Build | Maven 3.9 or later |

## Getting started

1. Install Java 25 (Temurin), Maven 3.9 or later, and a container runtime (Docker, Podman or Rancher Desktop).
2. Start the development database:

   ```bash
   export STATEMINT_DB_PASSWORD=choose-a-local-password
   docker compose up -d
   ```

   On Windows PowerShell use `$env:STATEMINT_DB_PASSWORD = "choose-a-local-password"`.
3. Build and test:

   ```bash
   mvn verify
   ```

4. Run the application (applies database migrations on start):

   ```bash
   mvn spring-boot:run
   ```

## Rules for every change

- Money is `BigDecimal` only; never `double` or `float` in the data path.
- Business dates are `LocalDate`; timestamps are stored in UTC.
- Every step must be safe to repeat after a crash.
- Never log field values, names or addresses: IDs only.
- Never commit client data, sample files with real customers, or secrets. Use masked or synthetic data.
- Database migrations are forward-only and must stay compatible with the previous release.

## Continuous integration

Every pull request builds and tests on Linux, Windows and macOS with Java 25, and applies every database migration to a fresh PostgreSQL 18. Renovate opens dependency upgrade pull requests every Monday and security fixes immediately.
