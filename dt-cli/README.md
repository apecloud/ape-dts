# dtscli

`dtscli` is a local command-line helper for creating and managing ApeCloud DTS tasks.
It generates `task_config.ini` files, starts `dt-main`, records local task metadata,
and provides simple commands for task listing, logs, stop, delete, and version checks.

## Installation

The installer installs the `dtscli` binary into `INSTALL_DIR` (`$HOME/.local/bin` by default).
It first looks for `./dtscli` in the current directory. If it is not present, pass the binary path explicitly:

```sh
sh scripts/install.sh /path/to/dtscli
```

After installation, make sure the install directory is in `PATH`.

The installer also writes shell completion files for Bash and Fish:

```text
$HOME/.local/share/bash-completion/completions/dtscli
$HOME/.config/fish/completions/dtscli.fish
```

For Zsh, the installer prefers `$ZSH_CUSTOM/completions/_dtscli` when Oh My Zsh is
installed, then the Homebrew `share/zsh/site-functions` directory on macOS, and
falls back to `$HOME/.zfunc/_dtscli`.

You can also print completions manually:

```sh
dtscli completion bash
dtscli completion fish
dtscli completion zsh
```

Enable Bash completion for the current shell:

```sh
source <(dtscli completion bash)
```

Install Bash completion permanently on macOS:

```sh
dtscli completion bash > "$(brew --prefix)/etc/bash_completion.d/dtscli"
```

Enable Fish completion for the current shell:

```fish
dtscli completion fish | source
```

Install Fish completion permanently:

```fish
dtscli completion fish > "$HOME/.config/fish/completions/dtscli.fish"
```

Enable Zsh completion for the current shell:

```zsh
source <(dtscli completion zsh)
```

Install Zsh completion permanently on macOS:

```zsh
dtscli completion zsh > "$(brew --prefix)/share/zsh/site-functions/_dtscli"
```

## Workspace

`dtscli` starts `dt-main` from a configured workspace. The workspace should contain:

```text
dt-main
dtscli
log4rs.yaml
```

Configure it with:

```sh
dtscli config set --workspace /path/to/ape-dts-release
```

Show the current config:

```sh
dtscli config get
```

By default, local state is stored under `$HOME/.ape-dts`. Set `APE_DTS_HOME` to use another location.

## Create a Task

Create and start a task with:

```sh
dtscli create \
  --name order_sync \
  --mode snapshot \
  --source mysql://user:password@127.0.0.1:3306 \
  --target mysql://user:password@127.0.0.1:3307 \
  --do test_db.*
```

Supported modes:

- `struct`
- `snapshot`
- `cdc`

Use `--preflight` to run checks for the selected mode without starting the task:

```sh
dtscli create \
  --name order_struct_preflight \
  --mode struct \
  --preflight \
  --source mysql://user:password@127.0.0.1:3306 \
  --target mysql://user:password@127.0.0.1:3307 \
  --do test_db.*
```

Preflight runs in the foreground, streams output until the process finishes, and
does not create persistent CLI task metadata. Press `Ctrl-C` to stop it.

Preflight generates mode-specific checks:

- `struct`: `do_struct_init=true`, `do_cdc=false`
- `snapshot`: `do_struct_init=false`, `do_cdc=false`
- `cdc`: `do_struct_init=false`, `do_cdc=true`

Supported database URL schemes:

- `mysql://`
- `postgres://`, `postgresql://`, `pg://`
- `mongodb://`, `mongo://`
- `redis://`

Use `--do` and `--ignore` to filter databases/schemas or tables/collections.
Separate expressions with commas. Use `db` for a database/schema and `db.table`
for a table/collection:

```sh
dtscli create ... \
  --do 'test_db,test_db.orders,`heh.e`.`ta,ble`' \
  --ignore 'test_db.tmp_*'
```

Quote the shell argument when escaped identifiers contain backticks or commas.
The generated config keeps escaped expressions intact, for example:

```ini
do_dbs=test_db
do_tbs=test_db.orders,`heh.e`.`ta,ble`
ignore_tbs=test_db.tmp_*
```

Use `--dry-run` to print the generated `task_config.ini` without creating files or starting `dt-main`:

```sh
dtscli create ... --dry-run
```

Use `--set section.key=value` to override generated config values:

```sh
dtscli create ... \
  --set parallelizer.parallel_size=8 \
  --set pipeline.buffer_size=64000
```

Start `dt-main` with an existing `task_config.ini`:

```sh
dtscli create --name order_from_file --file ./task_config.ini
```

`--name` is still required for local task tracking. Configuration-generation
options such as `--mode`, `--source`, `--target`, `--do`, and `--set` cannot be
combined with `--file`.

## Manage Tasks

List local tasks:

```sh
dtscli list
```

Show task metadata:

```sh
dtscli show order_sync
```

Print task logs:

```sh
dtscli logs order_sync
dtscli logs order_sync --follow
dtscli logs order_sync --file stderr
```

Start a stopped task with its recorded config file:

```sh
dtscli start order_sync
```

Stop a running task:

```sh
dtscli stop order_sync
```

Delete a stopped task record and local task files:

```sh
dtscli delete order_sync
```

Enter the task name when prompted to confirm deletion.

Use `--force` to stop a running task before deletion:

```sh
dtscli delete order_sync --force
```

## Version

Print the CLI version, configured workspace, detected binaries, and `dt-main` version:

```sh
dtscli version
```

If the detected `dt-main` version is lower than the required ApeCloud DTS version,
`dtscli` prints a warning but does not block execution.
