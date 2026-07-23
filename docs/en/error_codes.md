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
- Stage and endpoint are explicitly attached by execution boundaries; reports
  never infer either field from a code, provider, or module name.
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
| `RT002` | `OperationInterrupted` | The requested operation was interrupted |
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

Component-authored `message` and `hint` values remain free text. `detail` is
assembled by `ErrorReport` from ordinary `anyhow::Context` values and the
concrete cause chain, from outermost to innermost, with duplicate text removed.
The report boundary uses `rtb-redact` to remove credentials, authenticated URL
userinfo, authorization values, provider tokens, JWTs, long opaque tokens, and
private keys before any of these values enter the user view.

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
typed metadata frame whose code, message, hint, stage, task ID, endpoint role,
database object, and provider origin are all optional. It is a plain data
object and implements neither `Display` nor `std::error::Error`. The endpoint
role is `source`, `destination`, or `metadata`. Database objects may include
schema, table, column, and constraint names.

The innermost cause is always a real error. Provider failures keep their
original provider error type. Application-authored failures use the
`thiserror`-based `DtError` enum.

Project-owned failures select a semantic `DtError` variant and add only the
metadata known at that call site. For example,
`DtError::InvalidConfig(detail).stage(Stage::Bootstrap)` creates the root
cause and adds its stage; it does not repeat `ErrorCode::InvalidConfig`.
The `ClassifyError` implementation maps every `DtError` variant to a
`DtErrorContext` containing its stable default code and any metadata intrinsic
to that variant.
Adding a new variant therefore requires an explicit classifier arm and cannot
silently fall through. `DtError::Unclassified` is reserved for a project-owned
failure for which no stable classification is available.

The error extension trait is implemented for `anyhow::Error` and the supported
provider error types. `DtResultExt` provides the same `code`, `message`, `hint`,
`stage`, `task_id`, `endpoint`, `object`, and `origin` methods directly on
`Result`. Its `dt_context` method accepts a closure, so metadata is not built on
the successful path. A code attached to a provider result is an operation
fallback: a recognized provider code takes priority, while an unrecognized
provider condition retains the operation code. Project-owned failures should
normally use a semantic `DtError` variant. Unknown concrete errors retain their
source type when converted to `anyhow::Error`.
When a project-owned classification also has a lower-level source, put the
semantic `DtError` in the `anyhow` context chain above that source. Both the
`DtError` and the original source then remain downcastable, and the report can
classify both without discarding the source diagnostics.

Metadata is added as the error crosses ownership boundaries. A leaf frame can
contain the business code, affected object, and provider origin. Stage and
endpoint are attached at the narrowest component boundary that owns the
operation, while task ID is attached at the highest boundary that knows it:
for example, the extractor worker attaches
`extractor/source`, the common sinker adapter attaches `sinker/destination`,
the recovery initialization boundary attaches `resumer/metadata`, and the
precheck entry attaches `precheck` and the task ID while its builder attaches
`source` or `destination` around each checker call. The normal task entry is
the only production boundary that attaches its task ID. No report-time
inference is used.

Each frame is flat and immutable. The context extension appends frames to an
internal ordered list carried by `anyhow::Error`; `DtErrorContext` itself has
no parent link and no field-resolution methods. `ErrorReport` collects every
field into ordered arrays from explicit frames, project errors, and all known
provider causes, then applies display-specific precedence. Scope and
root-cause fields use the nearest inner value: stage, endpoint, and provider
origin are not replaced merely because an error passes through another
component. Error object fields are merged individually, preserving an inner
value and filling only missing fields from outer frames. Recognized provider
codes precede operation fallback codes; user message, hint, and task ID retain
explicit outer precedence. Consequently, a sinker
failure propagated through the parallelizer and pipeline is reported as
`sinker/destination`, while the task entry can still supply its task ID and
user-facing context.
`DtErrorContext` has no detail field. Report construction derives
`detail` from the redacted ordinary contexts and concrete causes in the error
chain. Project-owned `DtError` values retain their variant-specific full
`Display` text and typed payload, just as provider errors retain their own
`Display`, source chain, and concrete type.

Supported provider errors should normally be propagated unchanged with `?` or
ordinary `anyhow::Context`. The shared raw-cause registry classifies their
preserved concrete types when the report is built, recovering intrinsic code,
origin, and object fields. Attach an explicit `DtErrorContext` only for business
semantics or execution scope that the provider error cannot know. Neither path
infers stage, endpoint, or task ID at report time.

`ErrorReport` is the user-facing boundary representation. Its text form uses
`ERROR`, `TASK`, `AFFECTED`, `DETAIL`, and `HINT` lines.

Provider classifier implementations live under `dt-common::error::provider`.
They implement `ClassifyError` using provider-native codes, typed error kinds,
and Rust error variants; they must not parse provider messages. Each
implementation returns a `DtErrorContext` containing an optional recognized
code, provider origin, and affected object. A small explicit registry applies
the same trait to supported provider causes preserved in `anyhow::Error`.
The SQLx classifier infers MySQL or PostgreSQL from the concrete database error;
transport errors use the generic `sqlx` origin. Call sites do not pass a
database-family hint or attach a separate provider frame. A recognized provider
condition takes priority over an operation fallback code. Stage, endpoint, and
task ID are attached separately at the highest execution layer that knows them.

Provider classification belongs in `dt-common::error::provider`, not in
per-crate wrappers. Business modules use `DtError` for project-owned failures,
ordinary `anyhow::Context` for diagnostic prose, and the context extension
methods only for explicit metadata. Do not introduce per-crate helper modules
that merely forward arguments to these shared mechanisms.

`ErrorReport` reads the typed frame list and error chain. It appends values from
the explicit frames, a project `DtError`, and every registered raw provider
cause to separate per-field arrays. Field-specific selection and merge rules
exist only in the report layer. A known provider error therefore does not become
`IN999` or lose its precise code merely because a caller used a plain `?` or an
operation fallback, while explicit user-facing and scope metadata remains
available. Unsupported or unrecognized raw errors still become `IN999`.
Failure-path tests and code review remain necessary for explicit operation
codes and call-site metadata that no raw provider error can supply.

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

SQLx call sites may explicitly supply an operation-specific code such as
`CN001`, `DB001`, or `ST001`. A recognized provider classification takes
precedence; otherwise the operation code is retained. The provider origin,
constraint/table metadata, and source error chain are preserved in both cases.

The `tokio-postgres` replication adapter uses the same PostgreSQL SQLSTATE
rules. URL parsing uses `CF002`, connection establishment uses `CN001`, and
rejected replication commands use the operation-specific explicit code.

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
| MongoDB invalid client options / invalid TLS configuration | explicit call-site code `CF002` / `CN003` |
| MongoDB DNS, I/O, pool-cleared, server-selection, or shutdown | `CN001` or `CN002` for I/O timeout |
| Kafka authentication / authorization | `AU001` / `AU002` |
| Kafka unknown topic or partition | `MD001` |
| Kafka timeout / broker transport failure | `CN002` / `CN001` |
| HTTP request timeout / connection failure | `CN002` / `CN001` |
| HTTP non-success response or invalid response body | `DB001` |
| MySQL binlog I/O timeout / other transport failure | `CN002` / `CN001` |
| MySQL binlog invalid GTID | `CF002` |
| MySQL binlog error `1236` (requested binlog unavailable) | `ST001`, origin `mysql/1236` |
| Other MySQL binlog decoding failures | `DB001` |

Worker join failures are `RT001`. URL and YAML parsing errors explicitly
classified by their callers are `CF002`. `DtError::Unclassified` and an unsupported
or unrecognized raw error reaching `ErrorReport` are `IN999`. A local filesystem
`std::io::Error` is `IO001`; network I/O should retain its provider error type
so that it becomes `CN001` or `CN002` instead. User-interrupted CLI
operations are `RT002`.

`mysql-binlog-connector-rust v0.3.4` discards the numeric code from MySQL error
packets and exposes error `1236` as `ConnectError(String)`. Until the connector
preserves that typed code, the MySQL binlog classifier recognizes only the
known "requested binlog unavailable" messages and restores origin code `1236`.
This narrow message-based exception compensates for type information lost by
the provider library; other provider classification remains type- and code-based.

A malformed database connection URL is configuration failure `CF002`. `CN001`
is used only after a syntactically valid endpoint cannot be reached or an
established connection is lost. This distinction keeps the user action clear:
correct the config for `CF002`; investigate service and network reachability for
`CN001`.
