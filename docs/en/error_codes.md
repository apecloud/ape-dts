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
HINT: Check object routing and create the required object or enable structure initialization.
```

`MD001` is the only error identity. Stage is independent diagnostic metadata.
Application logic must compare the typed `ErrorCode`; it must not parse
messages or combine code and stage into another identifier.

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
user message, detail, hint, task ID, endpoint role, and affected object. The
text view includes these fields before the diagnostic section.

Internal diagnostics are available on the in-memory report as `stage`,
`origin`, the redacted `error_chain`, its `context_count`, and an optional
captured `backtrace`. These fields are excluded from JSON.
`ErrorReport` text rendering always includes all available diagnostics after
the user-facing section. API consumers must use the versioned JSON view rather
than parse CLI text.

Component-authored `message`, `detail`, and `hint` values remain free text. The
report boundary uses `rtb-redact` to remove credentials, authenticated URL
userinfo, authorization values, provider tokens, JWTs, long opaque tokens, and
private keys before they enter the user view. The text renderer applies the
same redaction to provider messages and `anyhow::Context` values.

CLI failures include a diagnostic section by default:

```text
DIAGNOSTIC [MD001]
STAGE: sinker
ENDPOINT: destination
ORIGIN: postgres/42P01
CONTEXT 1: starting task
CAUSE 1: relation missing
BACKTRACE:
...
```

Diagnostics can include SQL, row data, object names, provider messages, and
`anyhow::Context` values. Credential-shaped values and authenticated URLs are
redacted, but stderr and captured logs must still be treated as sensitive data.
The process never enables backtraces itself. `RUST_LIB_BACKTRACE=0` disables
error backtraces; any other configured value enables them. When that variable
is unset, `RUST_BACKTRACE` is interpreted by the same rule. The current error
report renders captured backtraces in the short format. Without a captured
backtrace, the complete structured diagnostic and error chain are still
printed and only the `BACKTRACE` block is omitted.

## Structured errors

`anyhow::Error` is the only error transport container. `DtErrorContext` is a
typed metadata frame whose code, message, detail, hint, stage, task ID,
endpoint role, database object, and provider origin are all optional. It
implements `Display` but intentionally does not implement
`std::error::Error`. The endpoint role is `source`, `destination`, or
`metadata`. Database objects may include schema, table, column, and constraint
names.

The innermost cause is always a real error. Provider failures keep their
original provider error type. Application-authored failures use the
`thiserror`-based `DtError` enum.

Project-owned failures construct the root cause first and add one immutable
metadata frame per fluent `DtErrorContextExt` call. For example,
`DtError::ConfigError(detail).with_code(ErrorCode::InvalidConfig).with_stage(Stage::Bootstrap)`.
The same extension trait is implemented for `anyhow::Error` and the supported
provider error types. Unknown concrete errors use `DtErrorContext::attach` at
the component boundary.

`DtError` variants describe the concrete project-owned root cause; they do not
imply or classify an `ErrorCode`. A single variant can be used with several
codes, so the failure site must attach the explicit business code. Do not add a
`DtError` classifier that guesses the code from the variant.

Metadata is added as the error crosses ownership boundaries. A leaf frame can
contain code, detail, object, and origin; a component frame adds stage and
endpoint; the task entry adds task ID through
`DtErrorContextExt::with_task_id`. Frames are immutable after attachment.
`ErrorReport` starts at the outermost frame and recursively reads its metadata
parents. For every field, the first non-empty outer value wins and a missing
value is inherited from the inner frame.
If no frame supplies a detail, report construction uses the project-owned
`DtError` payload as the detail. Provider messages remain diagnostic causes.

At a component boundary, a raw provider error is first classified by borrowing
it, then attached with `DtErrorContext::attach`. The resulting `anyhow::Error`
can be downcast both to `DtErrorContext` and to the original provider error
type. Once a provider error has been erased to `anyhow::Error`, it must not be
classified again; classification belongs at the first boundary that still has
the concrete provider type.

`ErrorReport` is the user-facing boundary representation. Its text form uses
`ERROR`, `TASK`, `AFFECTED`, `DETAIL`, and `HINT` lines.

Provider classifiers live under `dt-common::error::provider`. A classifier
uses provider-native codes, typed error kinds, and Rust error variants; it must
not parse provider messages. `ProviderErrorClassification::into_context`
creates a metadata frame containing an optional recognized code, provider
origin, and affected object. The component boundary first attaches its
operation-specific default code, then attaches the provider frame so a
recognized provider condition overrides that default. Stage and endpoint are
added outside both frames, while the provider error itself remains unchanged.

Component-specific metadata belongs in a small wrapper at the component
boundary. The component assigns the operation-specific default code, stage,
and endpoint. Provider classifiers never accept a business default: an
unrecognized provider condition returns no code and inherits the boundary
default. Task-level metadata is attached only at the task entry.

Each crate keeps all of its component wrappers in one `src/error_boundary.rs`
file and separates ownership with nested modules such as `extractor_error`, `sinker_error`,
or provider-specific modules. Do not add component error helper files beside
business modules. `error_boundary` means the point where a lower-level failure
is classified or enriched for Ape-DTS; it is distinct from the public error
contract in `dt-common::error`. Shared provider classifiers remain in
`dt-common::error::provider`.

`ErrorReport` only reads the typed context and error chain; it never attempts
provider classification. An error that reaches the report without a
`DtErrorContext` deterministically becomes `IN999`. Failure-path tests and the
code review expose missed classification sites.

`MD099 MetadataReadFailed` is only the default for reading or decoding endpoint
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

Every SQLx boundary also supplies an operation-specific default such as
`CN001`, `DB001`, or `ST001`. A recognized provider classification overrides
that default; otherwise the error inherits it. The original provider code,
constraint/table metadata, and source error chain are always preserved.

The `tokio-postgres` replication adapter uses the same PostgreSQL SQLSTATE
rules. URL parsing uses `CF002`, connection establishment uses `CN001`, and
rejected replication commands use the operation-specific boundary default.

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
| MongoDB invalid client options / invalid TLS configuration | boundary default `CF002` / `CN003` |
| MongoDB DNS, I/O, pool-cleared, server-selection, or shutdown | `CN001` or `CN002` for I/O timeout |
| Kafka authentication / authorization | `AU001` / `AU002` |
| Kafka unknown topic or partition | `MD001` |
| Kafka timeout / broker transport failure | `CN002` / `CN001` |
| HTTP request timeout / connection failure | `CN002` / `CN001` |
| HTTP non-success response or invalid response body | `DB001` |

Worker join failures are `RT001`. URL and YAML parsing errors classified at
their component boundary are `CF002`. An unclassified raw error reaching
`ErrorReport` is `IN999`. A local filesystem `std::io::Error` is `IO001`; network
I/O must be classified at its provider boundary so that it becomes `CN001` or
`CN002` instead.

A malformed database connection URL is configuration failure `CF002`. `CN001`
is used only after a syntactically valid endpoint cannot be reached or an
established connection is lost. This distinction keeps the user action clear:
correct the config for `CF002`; investigate service and network reachability for
`CN001`.
