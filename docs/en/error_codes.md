# Error codes

Ape-DTS task-runtime errors have a stable five-character condition code. The
code identifies what happened; runtime location is recorded separately as a
stage. In schema v1, both `dt-main` and `dtscli` render failures through the same
user-facing error report boundary.

For example:

```text
ERROR REPORT
  [MD001]: A required source or destination object was not found
  AFFECTED OBJECT: schema=public, table=orders
  CAUSED BY:
    0: postgres/42P01: relation does not exist
```

Each code is a stable condition identity, and one report contains one final
code. Stage is independent metadata. Application logic must inspect the typed
`ErrorCode`; it must not parse
messages or combine a code and stage into another identifier.

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
`schema_version` is `1`. Code, stage, task ID, and endpoint role are scalar.
User messages, details, hints, and affected objects are arrays. Code, stage,
and endpoint are resolved from innermost to outermost; task ID is resolved from
outermost to innermost. A recognized concrete error owns the code, and an
explicit context code is a fallback when the concrete error is unclassified.
Arrays preserve outermost-to-innermost first-seen order and remove exact
duplicates. Text rendering always uses zero-based detail indexes. CLI text
layout is not a stable machine interface. Messages appear after the bracketed
code and are separated by semicolons. Fields within one affected object are
separated by commas, while multiple affected objects are separated by
semicolons.

The serialized `ErrorReport` no longer stores `error_chain` or `context_count`.
It includes its UTC creation `timestamp` and an optional captured `backtrace` in
addition to the user-facing fields.
`details` collects ordinary `anyhow::Context` values and concrete causes from
outermost to innermost; the internal metadata marker is omitted. The report
boundary uses `rtb-redact` to remove credentials, authenticated URL
userinfo, authorization values, provider tokens, JWTs, long opaque tokens, and
private keys before any of these values enter the user view.

Text output shows the code and messages, optional affected objects, details
under `CAUSED BY`, and an optional backtrace:

```text
ERROR REPORT
  [DB001]: A source or destination operation failed
  AFFECTED OBJECT: schema=public, table=orders, constraint=orders_pkey
  CAUSED BY:
    0: starting task
    1: postgres/42P01: relation does not exist
  BACKTRACE:
    0: dt_task::task_runner::TaskRunner::start_task
```

Details can include SQL, row data, object names, provider messages, and
`anyhow::Context` values. Credential-shaped values and authenticated URLs are
redacted, but stderr and captured logs must still be treated as sensitive data.
`dt-main` writes the text form to `default.log` and additionally appends the
complete JSON form to `error_report.log`. Each `error_report.log` record starts
with a UTC logger timestamp followed by ` | ` and the JSON string. The JSON also
contains the report creation timestamp; JSON escaping keeps a multi-line
backtrace within the same log record. This dedicated logger accepts every log
level and is independent of the task's configured runtime log level.

## Structured errors

`anyhow::Error` is the only error transport container. `DtErrorContext` is a
typed metadata frame whose code, message, detail, hint, stage, task ID,
endpoint role, and database object are all optional. It is a plain data
object and implements neither `Display` nor `std::error::Error`. The endpoint
role is `source`, `destination`, or `metadata`. Database objects may include
schema, table, column, and constraint names.

The innermost cause is always a real error. Provider failures keep their
original provider error type. Application-authored failures use the
`thiserror`-based `DtError` enum.

Project-owned failures select a semantic `DtError` variant and add only the
metadata known at that call site. For example,
`DtError::InvalidConfig(detail)` creates the root cause; its classifier supplies
`ErrorCode::InvalidConfig`, the detail, and `Stage::Bootstrap`.
The `ClassifyError` implementation maps every `DtError` variant to a
`DtErrorContext` containing its stable default code and any metadata intrinsic
to that variant.
Adding a new variant therefore requires an explicit classifier arm and cannot
silently fall through. `DtError::Unclassified` is reserved for a project-owned
failure for which no stable classification is available.

The error extension trait is implemented for every error type that can convert
into `anyhow::Error`. `DtResultExt` provides the same `code`, `message`, `hint`,
`stage`, `task_id`, `endpoint`, and `object` methods directly on `Result`. Its
`dt_context` method accepts a closure, so metadata is not built on the successful
path. Project-owned failures should normally use a semantic `DtError` variant.
Unknown concrete errors retain their source type when converted to
`anyhow::Error`.
When a project-owned classification also has a lower-level source, put the
semantic `DtError` in the `anyhow` context chain above that source. Both the
`DtError` and the original source then remain downcastable, and the report can
classify both without discarding the source diagnostics.

Metadata is added as the error crosses ownership boundaries. A leaf frame can
contain the business code and affected object. Stage and
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
no parent link. `ErrorReport` resolves code, stage, and endpoint from innermost
to outermost, while task ID is resolved from outermost to innermost. Messages,
details, hints, and affected objects are appended in outermost-to-innermost
first-seen order with exact duplicates removed.
Classifiers primarily populate `DtErrorContext.detail`; ordinary contexts and
unclassified concrete causes also contribute redacted `details`.
Project-owned `DtError` values retain their variant-specific full
`Display` text and typed payload, just as provider errors retain their own
`Display`, source chain, and concrete type.

`ErrorReport` is the user-facing boundary representation. Its text form uses
the bracketed error code followed by semicolon-separated messages. When
present, affected objects are rendered on one line: fields within an object use
commas and multiple objects use semicolons. Details remain ordered and
zero-based under `CAUSED BY`, and a captured backtrace is appended as a
`BACKTRACE` block. The complete structured fields remain available in its JSON
form.

Provider classifier implementations live under `dt-common::error::provider`.
They implement `ClassifyError` using provider-native codes, typed error kinds,
and Rust error variants; they must not parse provider messages. Each
implementation returns a `DtErrorContext` containing an optional recognized
code, affected object, and complete provider detail. A small chain registry in
the parent-level `dt-common::error::classifier` applies the same trait to
supported provider causes preserved in the source chain.
The SQLx classifier infers MySQL or PostgreSQL from the concrete database error.
Call sites do not pass a database-family hint or attach a separate provider
frame. A recognized provider condition owns the identity; an explicit
operation code is used only as a fallback for an unclassified provider error.
Provider name, original code, and concrete error text are stored in `details`.
Stage, endpoint, and task ID are attached separately at the execution layer
that knows them.

Provider classification belongs in `dt-common::error::provider`, not in
per-crate wrappers. Business modules use `DtError` for project-owned failures,
ordinary `anyhow::Context` for diagnostic prose, and the context extension
methods only for explicit metadata. Do not introduce per-crate helper modules
that merely forward arguments to these shared mechanisms.

`dt-common::error::classifier` first collects typed frames, recognizes a typed
`DtError` through `anyhow::Error::downcast_ref`, and then traverses the source
chain to classify concrete causes. A classifier supplies provider detail
directly; an unclassified non-marker error uses its redacted `Display` as
detail. `ErrorReport` only applies the returned `DtErrorContext` values in
order. `IN999` is used when no code is available. Failure-path tests and code
review remain necessary for explicit operation codes and call-site metadata
that no raw provider error can supply.

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

Provider codes are diagnostic data, not Ape-DTS error codes. The original typed
provider error remains in the source chain, and its redacted display and source
text contribute to `DETAIL`; there is no separate provider-origin field. The
codes are also used internally for these initial SQLx mappings:

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

SQLx call sites may explicitly supply an operation-specific fallback code such
as `DB001` or `ST001`. A recognized provider classification owns the report
identity; the explicit code is used only when the provider error is
unclassified. The original provider code, constraint/table metadata, and source
error information remain available through detail and object fields.

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
| MySQL binlog error `1236` (requested binlog unavailable) | `ST001` |
| Other MySQL binlog decoding failures | `DB001` |

Worker join failures are `RT001`. URL parsing errors explicitly classified by
their callers are `CF002`. Logger YAML errors occur during startup and are
surfaced directly by the `dt-main` `expect` boundary. `DtError::Unclassified`
is `IN999`; an
unsupported or unrecognized raw error adds `IN999` only when the report contains
no other code. A local filesystem `std::io::Error` is `IO001`; network I/O should
retain its provider error type so that it becomes `CN001` or `CN002` instead.
User-interrupted CLI operations are `RT002`.

`mysql-binlog-connector-rust v0.3.4` discards the numeric code from MySQL error
packets and exposes error `1236` as `ConnectError(String)`. Until the connector
preserves that typed code, the MySQL binlog classifier recognizes only the
known "requested binlog unavailable" messages and classifies them as `ST001`.
The raw `ConnectError` text remains in `DETAIL`. This narrow message-based
exception compensates for type information lost by the provider library; other
provider classification remains type- and code-based.

A malformed database connection URL is configuration failure `CF002`. `CN001`
is used only after a syntactically valid endpoint cannot be reached or an
established connection is lost. This distinction keeps the user action clear:
correct the config for `CF002`; investigate service and network reachability for
`CN001`.
