# Error codes

Ape-DTS task-runtime errors have a stable five-character condition code. The
code identifies what happened; runtime location is recorded separately as a
stage. In v1, both `dt-main` and `dtscli` render failures through the same
user-facing error report boundary.

For example:

```text
ERROR [MD001]: A required database object was not found
TASK: sync-orders-01
AFFECTED: destination postgres sales.orders
PHASE: writing to the destination
HINT: Check object routing and create the required object or enable structure initialization.
```

`MD001` is the only error identity. Stage and operation are independent
diagnostic fields. Application logic must compare the typed `ErrorCode`; it
must not parse messages or combine code and stage into another identifier.
Operation is free-form diagnostic metadata and must never be used as a policy
key.

## Compatibility rules

- Codes are five uppercase ASCII letters or digits.
- Codes are append-only and must never be reused for a different condition.
- A category code ending in `000` is reserved and must not be emitted.
- The stage is not part of the stable error identity.
- Once a stage has been assigned, callers must not overwrite it.
- Retry, skip, fallback, and DLQ behavior are policy decisions outside the
  error-code contract.

The catalog is declared once in `dt-common::error` and generates the enum,
wire-code mapping, default message, default hint, and complete code list. New
codes must be added to that declaration rather than maintained in parallel
matches.

## Code catalog

| Code | Condition | Default meaning |
|---|---|---|
| `CF001` | `MissingConfig` | Required configuration is missing |
| `CF002` | `InvalidConfig` | Configuration is invalid |
| `CF003` | `MissingConfigItem` | A required configuration item is missing |
| `CN001` | `ConnectionFailed` | An endpoint connection could not be established or was lost |
| `CN002` | `ConnectionTimeout` | Connection-pool acquisition timed out |
| `CN003` | `TlsFailed` | A secure database connection could not be established |
| `AU001` | `AuthenticationFailed` | Credentials were rejected |
| `AU002` | `PermissionDenied` | The authenticated account lacks permission |
| `PR001` | `PrerequisiteNotMet` | A required version, CDC setting, slot, or capability is unavailable |
| `PR002` | `UnsupportedDatabaseVersion` | The database version is unsupported |
| `PR003` | `CdcNotEnabled` | The source database is not configured for CDC |
| `PR004` | `ReplicationCapacityExhausted` | No replication slot or sender capacity is available |
| `PR005` | `UnsupportedTableStructure` | A table structure is unsupported |
| `MD001` | `ObjectNotFound` | A required table, topic, or other endpoint object does not exist |
| `MD002` | `DatabaseNotFound` | A database does not exist |
| `MD099` | `MetadataReadFailed` | Database information required by the migration could not be read |
| `DB001` | `StatementFailed` | A source or destination operation failed |
| `IC001` | `IntegrityViolation` | A constraint was violated |
| `ST001` | `CheckpointReadFailed` | Checkpoint state could not be read |
| `IO001` | `IoFailed` | An I/O operation failed |
| `RT001` | `WorkerFailed` | A task worker terminated unexpectedly |
| `IN001` | `InvariantViolated` | An internal invariant was violated |
| `IN999` | `Unclassified` | No stable classification is available yet |

## Diagnostic stage catalog

| Stage | Meaning |
|---|---|
| `bootstrap` | Loading task configuration |
| `precheck` | Checking migration prerequisites |
| `extractor` | Reading from the source |
| `parallelizer` | Preparing migration work |
| `pipeline` | Processing migration data |
| `sinker` | Writing to the destination |
| `checker` | Checking migrated data |
| `resumer` | Restoring saved task progress |
| `task` | Running the migration task |
| `unknown` | No structured stage is available |

## User and diagnostic views

`ErrorReport` JSON is a versioned machine interface. The current
`schema_version` is `1`. Adding optional fields is compatible; removing,
renaming, or changing the meaning of an existing user-facing field requires a
new schema version. CLI text layout is not a stable machine interface.

The `ErrorReport` JSON view contains only user-facing fields: the stable code,
user message, detail, hint, task ID, endpoint role, affected object, and a
human-readable phase. The text view includes those fields followed by the
diagnostic section.

Internal diagnostics are available on the in-memory report as
`stage`, `operation`, `origin`, `contexts`, `diagnostic_message`, and the
captured source `location`. `ErrorReport` text rendering always includes these
fields after the user-facing section. API consumers must use the versioned JSON
view rather than parse CLI text.

Component-authored `message`, `detail`, and `hint` values remain free text. The
report boundary uses `rtb-redact` to remove credentials, authenticated URL
userinfo, authorization values, provider tokens, JWTs, long opaque tokens, and
private keys before they enter the user view. The text renderer applies the
same redaction to provider messages and `anyhow::Context` values.

CLI failures include a diagnostic section by default:

```text
DIAGNOSTIC [MD001]
LOCATION: dt-connector/src/sinker/pg/pg_sinker.rs:250:22
STAGE: sinker
OPERATION: sink_dml
ENDPOINT: destination
ORIGIN: postgres/42P01
```

Diagnostics can include SQL, row data, object names, provider messages, and
`anyhow::Context` values. Credential-shaped values and authenticated URLs are
redacted, but stderr and captured logs must still be treated as sensitive data.

## Structured errors

`DtError` can record the condition code, message, detail, hint, stage,
operation, task ID, endpoint role, database object, provider origin, and source
error. The endpoint role is `source`, `destination`, or `metadata`. Database
objects may include schema, table, column, and constraint names.

`DtError` is the only application error type in `dt-common::error`; there is no
legacy compatibility enum. New application-authored failures must construct a
`DtError`. Raw provider errors may cross only a short boundary before a
provider adapter or `ErrorReport` classifies them.

The root code, stage, operation, and origin use first-writer semantics. A
component that receives an existing structured error preserves this root
context. Additional propagation details use the `anyhow::Context` and source
error chains available in the diagnostic view.

A single `DtError` always has one primary code. Components that already report
multiple independent results, such as precheck, keep that aggregation in their
own result type rather than adding it to the common error contract.

`ErrorReport` is the user-facing boundary representation. Its text form uses
`ERROR`, `TASK`, `AFFECTED`, `PHASE`, `DETAIL`, and `HINT` lines.

Provider classifiers and adapters live under
`dt-common::error::provider`. A classifier uses provider-native codes, typed
error kinds, and Rust error variants; it must not parse provider messages. An
adapter attaches the provider origin and source chain but does not set stage,
endpoint, or operation.

Component-specific enrichment belongs in a small wrapper at the component
boundary. The first Ape-DTS component boundary assigns the root stage and
endpoint, while the call site supplies the operation and fallback code. Later
boundaries preserve the root error. These wrappers must use `#[track_caller]`
so the captured location continues to identify the failing component
operation.

Each crate keeps all of its component wrappers in one `src/error_boundary.rs`
file and separates ownership with nested modules such as `extractor`, `sinker`,
or provider-specific modules. Do not add component error helper files beside
business modules. `error_boundary` means the point where a lower-level failure
is classified or enriched for Ape-DTS; it is distinct from the public error
contract in `dt-common::error`. Shared provider classifiers remain in
`dt-common::error::provider`.

`MD099 MetadataReadFailed` is only the fallback for reading or decoding endpoint
catalog and control metadata. A schema object being migrated does not make every
structure error a metadata-read failure. Missing objects use `MD001`/`MD002`,
unsupported structures use `PR005`, unmet version or topology requirements use
`PR001`/`PR002`, and rejected destination DDL uses `DB001`.

Classify errors by what the user can verify or change, not by the internal module
or data structure that detected the failure. A missing schema, table, or column
uses `MD001` and identifies the affected object. A CDC row event without its
matching table or relation definition uses `DB001` and tells the user how to
restart from an earlier log position. An incomplete Redis slot map uses `PR001`
and tells the user to stabilize the cluster topology. Use `IN001` only when no
endpoint, configuration, or migration action can reasonably resolve the failure.
Internal terms such as cache entry, OID map, or Rust type must never be the only
explanation. The user message and hint must state the affected endpoint behavior
and the next action. `DETAIL` may include a provider identifier such as a table or
relation ID when it helps correlate the failure with provider logs.

## Provider error preservation

Provider codes are diagnostic data, not Ape-DTS error codes. They are retained
under `origin` whenever available. Initial SQLx mappings are:

| Provider error | Ape-DTS code |
|---|---|
| PostgreSQL `42P01`/`42703`/`42704`, MySQL `1054`/`1146` | `MD001` |
| PostgreSQL `3D000`, MySQL `1049` | `MD002` |
| PostgreSQL SQLSTATE class `28`, MySQL `1045` | `AU001` |
| PostgreSQL `42501`, MySQL `1044`/`1142`/`1143`/`1227`/`1370` | `AU002` |
| PostgreSQL SQLSTATE class `08` | `CN001` |
| PostgreSQL SQLSTATE class `23` | `IC001` |
| MySQL `2002`/`2003`/`2006`/`2013` | `CN001` |
| SQLx connection configuration error | `CF002` |
| SQLx I/O, protocol, or closed pool | `CN001` |
| SQLx pool timeout | `CN002` |
| SQLx TLS error | `CN003` |
| SQLx worker crash | `RT001` |
| SQLx integrity error kinds | `IC001` |

Every SQLx boundary also supplies an operation-specific fallback such as
`CN001`, `DB001`, or `ST001`. The original provider code, constraint/table
metadata, and source error chain are preserved even when the fallback is used.

The `tokio-postgres` replication adapter uses the same PostgreSQL SQLSTATE
rules. URL parsing uses `CF002`, connection establishment uses `CN001`, and
rejected replication commands use the operation fallback.

Other initial provider mappings are:

| Provider error | Ape-DTS code |
|---|---|
| Redis authentication / `NOAUTH` / `WRONGPASS` | `AU001` |
| Redis `NOPERM` / `READONLY` | `AU002` |
| Redis invalid client configuration | `CF002` |
| Redis timeout | `CN002` |
| Redis I/O, cluster down, master down, or missing cluster connection | `CN001` |
| MongoDB authentication or command code `18` | `AU001` |
| MongoDB command code `13` | `AU002` |
| MongoDB command code `26` | `MD001` |
| MongoDB duplicate-key codes `11000`/`11001`/`12582` | `IC001` |
| MongoDB invalid client options / invalid TLS configuration | boundary fallback `CF002` / `CN003` |
| MongoDB DNS, I/O, pool-cleared, server-selection, or shutdown | `CN001` or `CN002` for I/O timeout |
| Kafka authentication / authorization | `AU001` / `AU002` |
| Kafka unknown topic or partition | `MD001` |
| Kafka timeout / broker transport failure | `CN002` / `CN001` |
| HTTP request timeout / connection failure | `CN002` / `CN001` |
| HTTP non-success response or invalid response body | `DB001` |

Worker join failures are `RT001`. URL and YAML parsing errors at the report
boundary are `CF002`. A local filesystem `std::io::Error` is `IO001`; network
I/O must be classified at its provider boundary so that it becomes `CN001` or
`CN002` instead.

A malformed database connection URL is configuration failure `CF002`. `CN001`
is used only after a syntactically valid endpoint cannot be reached or an
established connection is lost. This distinction keeps the user action clear:
correct the config for `CF002`; investigate service and network reachability for
`CN001`.
