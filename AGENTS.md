# AGENTS guide for `ke-analytics`

This file is for agentic coding tools (like AI assistants) working in this repository. It describes how to build and test the service, and the coding conventions you should follow.

---

## 1. Project overview

- JVM service written in Kotlin using Spring Boot WebFlux.
- Build system: Maven (single module) with parent `dev.crashteam:service-parent-pom`.
- Kotlin sources live under `src/main/kotlin`.
- Database access via jOOQ code generation and Liquibase migrations.
- Uses PostgreSQL, ClickHouse, Redis, Quartz, and AWS-style Kinesis streams.
- Application configuration is in `src/main/resources/application.yaml`.

There are currently **no** repository-level Cursor or Copilot instruction files:

- No `.cursor/rules/` directory.
- No `.cursorrules` file.
- No `.github/copilot-instructions.md`.

If these files are added later, treat them as authoritative and update this document to reflect any additional constraints.

---

## 2. Build, run, and migrations

All commands assume the repo root: `ke-analytics`.

### 2.1 Standard build

- Full build with tests (preferred):
  - `mvn clean verify`
- Build executable JAR (tests included):
  - `mvn clean package`
- Build fast, skipping tests (for exploratory work only):
  - `mvn clean package -DskipTests`

The Spring Boot Maven plugin is configured; the packaged JAR is produced under `target/` (name is derived from `artifactId` and `version` in `pom.xml`).

### 2.2 Running the application locally

- Run via Maven (common during development):
  - `mvn spring-boot:run`
- Run from the built JAR (after `mvn package`):
  - `java -jar target/ke-analytics-0.0.1.jar`

Runtime requirements (based on `application.yaml`):

- Java 21.
- PostgreSQL accessible at `jdbc:postgresql://localhost:5432/ke-analytics` (user `postgres` / password `postgres` by default).
- ClickHouse reachable using the `clickhouse.url` and credentials in `application.yaml` or environment overrides.
- Redis instance for caching and streams.

Do **not** hard-code secrets into source files. Use environment variables to override any credentials from `application.yaml` in non-dev environments.

### 2.3 Liquibase and jOOQ code generation

This project uses:

- `liquibase-maven-plugin` to apply DB migrations.
- `jooq-codegen-maven` to generate jOOQ model classes into `target/generated-sources/jooq`.

Both are bound to the `generate-sources` phase. In practice:

- To re-run migrations and regenerate jOOQ models:
  - `mvn clean generate-sources`

Agents should **never** manually edit generated code under `target/generated-sources/jooq`; change the Liquibase changelogs instead.

---

## 3. Tests and how to run one test

At the moment there are no test sources under `src/test/kotlin`, but the Maven configuration includes JUnit 5 and Spring Boot testing dependencies. Use these conventions when adding or running tests.

### 3.1 Standard test commands

- Run unit tests:
  - `mvn test`
- Run tests as part of the full lifecycle (CI-equivalent):
  - `mvn clean verify`
- Skip tests entirely (not recommended on main branches):
  - `mvn clean verify -DskipTests`

PostgreSQL test support is provided by `pg-embedded-plugin`. It starts an embedded database during the build phases where it is configured; you normally do **not** need a separate manual DB for tests.

### 3.2 Running a single test class

Use Maven Surefire's `-Dtest` selector from the repo root:

- Single test class:
  - `mvn test -Dtest=UserServiceTest`
- Multiple test classes:
  - `mvn test -Dtest=UserServiceTest,UserControllerTest`

Name new test classes with the `*Test` suffix and place them under `src/test/kotlin` mirroring the main package structure.

### 3.3 Running a single test method

You can also narrow execution to a specific test method in a class:

- Single test method (JUnit 5):
  - `mvn test -Dtest=UserServiceTest#createsApiKey`

Use this pattern when iterating quickly on a failing or newly added test.

---

## 4. Linting, formatting, and static analysis

There are no explicit Kotlin/Java style plugins declared in this module `pom.xml` (such as ktlint, detekt, or Checkstyle). The parent POM may add additional plugins; they typically run during `mvn verify`.

Agents should:

- Prefer `mvn verify` before finalizing substantial changes to surface any parent-defined checks.
- Keep formatting consistent with the existing code, which follows IntelliJ IDEA default Kotlin style:
  - 4 spaces for indentation.
  - Braces on the same line as declarations.
  - Blank line between package and imports; between imports and declarations.

If a formatter is introduced later (e.g., ktlint or detekt), update this section to document the canonical lint command, such as:

- `mvn ktlint:format` (example only; **not** currently configured).

---

## 5. Kotlin and Spring style guidelines

### 5.1 Packages and layering

- Keep the current high-level package structure:
  - `dev.crashteam.keanalytics.controller` for REST controllers.
  - `dev.crashteam.keanalytics.service` for business logic.
  - `dev.crashteam.keanalytics.repository.postgres` for DB repositories.
  - `dev.crashteam.keanalytics.client.*` for external HTTP/gRPC clients.
  - `dev.crashteam.keanalytics.controller.model` for API-facing DTOs (`*View`, request/response models).
  - `dev.crashteam.keanalytics.extensions` for Kotlin extension utilities.
- Follow the existing layering:
  - Controllers → Services → Repositories → jOOQ models / JDBC.
  - Do **not** have controllers talk directly to jOOQ or JDBC APIs.

### 5.2 Naming conventions

- Classes and interfaces: `PascalCase` (`UserService`, `MarketDbApiController`).
- Functions and properties: `camelCase` (`getApiKey`, `generateRandomString`).
- DTOs:
  - View models returned from controllers end with `View` (e.g. `ProductView`).
  - Client models for external services use `Response`, `Request`, or domain-specific suffixes.
- Exceptions: end with `Exception` (e.g. `AuthorizationException`, `UserSubscriptionGiveawayException`).
- Test classes (when added): end with `Test`.

### 5.3 Imports

- Group imports by package, with no extra blank lines beyond the standard Kotlin style.
- Wildcard imports are currently used only for closely related internal packages (e.g. `controller.model.*`).
- When adding new code:
  - Prefer explicit imports for external libraries and framework types.
  - Avoid introducing new wildcards for broad packages like `kotlin.*`, `java.*`, or `org.springframework.*`.

### 5.4 Nullability and optionals

- Prefer non-nullable Kotlin types where possible.
- Use nullable types only when a value is genuinely optional.
- For REST APIs:
  - Favor clear HTTP status codes over returning `null` payloads.
  - For new endpoints, prefer `404`/`204` rather than `200 OK` with `null`, unless matching an existing contract.

### 5.5 Coroutines, WebFlux, and reactive code

- The project uses Spring WebFlux with Kotlin coroutines.
- Controller endpoints may be `suspend` functions that internally call suspending service methods.
- For filter and infrastructure layers that use Reactor types (`Mono`, `Flux`), keep APIs in terms of Reactor types and adapt at the boundaries when needed.
- When mixing Reactor and coroutines, prefer well-known adapters (e.g. `awaitSingle` / `awaitSingleOrNull`) instead of custom blocking.

### 5.6 Logging

- Use KotlinLogging for logging, following the existing pattern:
  - Top-level logger per file:
    - `private val log = KotlinLogging.logger {}`
- Log error conditions at `error` level, with the exception attached as a parameter.
- Avoid logging sensitive information (API keys, passwords, tokens, or full PII identifiers).

### 5.7 Error handling

- Use Spring's `@RestControllerAdvice` (see `RestExceptionHandler`) to map exceptions to consistent HTTP responses.
- For client errors (invalid input, missing resources):
  - Throw `IllegalArgumentException` or more specific domain exceptions and map them to `4xx` codes.
- For authorization/authentication problems (see `ApiKeyAuthHandlerFilter`, `AuthorizationException`):
  - Return `401` or `403` depending on the case.
- For domain-specific cases (e.g., API key already exists):
  - Prefer specific exceptions under `dev.crashteam.keanalytics.service.exception` and map them centrally instead of sprinkling response construction logic in multiple places.

---

## 6. Repository- and DB-specific notes for agents

- Do **not** edit files under `target/` or other generated directories; adjust sources or migrations instead.
- Liquibase changelogs live under `src/main/resources/db/changelog`.
  - When modifying the DB schema, add a new changelog file and include it in the main changelog YAML.
- Keep JOOQ models in sync by re-running `mvn generate-sources` after schema changes.
- When modifying API contracts (controller models or endpoints):
  - Update `controller.model` DTOs and any corresponding OpenAPI or proto definitions in shared modules.

---

## 7. Agent workflow recommendations

- Before making non-trivial changes:
  - Skim `pom.xml` and `application.yaml` for relevant configuration.
  - Run `mvn -q -DskipTests compile` to validate basic compilation if you change signatures.
- Before finalizing work:
  - Run `mvn clean verify` where feasible.
- When in doubt about style, copy nearby patterns from existing Kotlin files in the same package.

## 8. Architecture docs (LikeC4)

Source of truth: `docs/architecture/`

Rules:
- When the task is about system architecture, dependencies, integration, components, or flows:
  1) First read `docs/architecture/**/*.c4`
  2) Prefer LikeC4 docs over guessing from service code
  3) If the question is about one service, also read that service folder afterwards

This document is intended to be kept up to date. When repository-wide conventions or tools change (for example, adding ktlint, detekt, or Cursor rules), please extend this `AGENTS.md` rather than creating parallel instruction sources.
