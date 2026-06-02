use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{error::ErrorKind, Args, CommandFactory, Parser, Subcommand};
use clap_complete::generate;
use configparser::ini::Ini;
use serde::{Deserialize, Serialize};

mod config;

use config::{build_task_config, infer_db_type, CreateConfig, DbType, Mode};

const APP_NAME: &str = "dtscli";
const ABOUT: &str = "A Command Line Interface for ApeCloud DTS";
const REQUIRED_DTS_VERSION: &str = "v2.0.26";
static PREFLIGHT_INTERRUPTED: AtomicBool = AtomicBool::new(false);
const BASH_COMPLETION_AFTER_HELP: &str = r#"This script depends on the 'bash-completion' package.
If it is not installed already, you can install it via your OS's package manager.

To load completions in your current shell session:

  source <(dtscli completion bash)

To load completions for every new session, execute once:

Linux:

  dtscli completion bash > /etc/bash_completion.d/dtscli

macOS:

  dtscli completion bash > "$(brew --prefix)/etc/bash_completion.d/dtscli"

You will need to start a new shell for this setup to take effect."#;
const FISH_COMPLETION_AFTER_HELP: &str = r#"To load completions in your current shell session:

  dtscli completion fish | source

To load completions for every new session, execute once:

  dtscli completion fish > "$HOME/.config/fish/completions/dtscli.fish"

You will need to start a new shell for this setup to take effect."#;
const ZSH_COMPLETION_AFTER_HELP: &str = r#"To load completions in your current shell session:

  source <(dtscli completion zsh)

To load completions for every new session, execute once:

Linux:

  dtscli completion zsh > "${fpath[1]}/_dtscli"

macOS:

  dtscli completion zsh > "$(brew --prefix)/share/zsh/site-functions/_dtscli"

You will need to start a new shell for this setup to take effect."#;
const CREATE_AFTER_HELP: &str = r#"Examples:
  Minimal snapshot task:
    dtscli create \
      --name order_sync \
      --mode snapshot \
      --source mysql://user:password@127.0.0.1:3306 \
      --target mysql://user:password@127.0.0.1:3307 \
      --do test_db.*

  Run preflight checks for a struct task without starting the task:
    dtscli create \
      --name order_struct_preflight \
      --mode struct \
      --preflight \
      --source mysql://user:password@127.0.0.1:3306 \
      --target mysql://user:password@127.0.0.1:3307 \
      --do test_db.*

  Start a task from an existing task_config.ini:
    dtscli create --name order_from_file --file ./task_config.ini

  Advanced CDC task with explicit identity, filters, and low-level config overrides:
    dtscli create \
      --name order_cdc \
      --mode cdc \
      --source-db pg \
      --target-db pg \
      --source postgres://127.0.0.1:5432 \
      --source-user repl_user \
      --source-password repl_password \
      --target postgres://127.0.0.1:5433 \
      --target-user app_user \
      --target-password app_password \
      --do public,public.orders,public.order_items \
      --ignore public.tmp_* \
      --do-events insert,update,delete \
      --pg-slot-name ape_dts_order_cdc \
      --set extractor.heartbeat_interval_secs=5 \
      --set parallelizer.parallel_size=8 \
      --set pipeline.buffer_size=64000 \
      --set runtime.log_level=debug \
      --dry-run"#;

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("{err:#}");
        std::process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        print_root_help();
        return Ok(());
    }

    if matches!(args[0].as_str(), "-h" | "--help" | "help") {
        print_root_help();
        return Ok(());
    }

    let cli = match Cli::try_parse_from(std::iter::once(APP_NAME.to_string()).chain(args)) {
        Ok(cli) => cli,
        Err(err) => {
            let kind = err.kind();
            if matches!(kind, ErrorKind::MissingRequiredArgument) {
                eprint!("{}", clap_message(&err));
            } else {
                err.print()?;
            }
            if matches!(kind, ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) {
                return Ok(());
            }
            std::process::exit(2);
        }
    };
    if command_should_warn_workspace_binaries(&cli.command) {
        warn_if_workspace_binaries_missing_from_config();
    }
    match cli.command {
        Commands::Config(command) => handle_config(command)?,
        Commands::Create(create) => handle_create(create)?,
        Commands::Start(start) => handle_start(start)?,
        Commands::List => handle_list()?,
        Commands::Show(show) => handle_show(show)?,
        Commands::Logs(logs) => handle_logs(logs)?,
        Commands::Stop(stop) => handle_stop(stop)?,
        Commands::Delete(delete) => handle_delete(delete)?,
        Commands::Completion(args) => handle_completion(args),
        Commands::Version => print_version()?,
    }
    Ok(())
}

fn clap_message(err: &clap::Error) -> String {
    let rendered = err.to_string();
    rendered
        .strip_prefix("error: ")
        .unwrap_or(&rendered)
        .to_string()
}

fn print_root_help() {
    println!(
        r#"=============================================
  ____ _____ ____        ____ _     ___
 |  _ \_   _/ ___|      / ___| |   |_ _|
 | | | || | \___ \_____| |   | |    | |
 | |_| || |  ___) |_____| |___| |___ | |
 |____/ |_| |____/       \____|_____|___|

=============================================
{ABOUT}

Available Commands:
  config      Manage dtscli defaults.
  create      Create and start a DTS task.
  start       Start a stopped DTS task.
  list        List local DTS tasks.
  logs        Print or follow task logs.
  show        Show task details.
  stop        Stop a running task.
  delete      Delete a stopped task record and local task files.
  completion  Generate shell completion scripts.
  version     Print version information.

Usage:
  dtscli [command] [flags]

Use "dtscli <command> --help" for more information about a given command."#
    );
}

fn print_version() -> Result<()> {
    println!("{} {}", APP_NAME, env!("CARGO_PKG_VERSION"));
    match load_cli_config().and_then(|cfg| resolve_workspace(&cfg)) {
        Ok(workspace) => {
            println!("workspace {}", workspace.display());
            let binaries = resolve_workspace_binaries(&workspace);
            if let Some(cli) = binaries.cli.as_ref() {
                println!("dtscli path {}", cli.display());
            } else {
                println!("dtscli not found under workspace {}", workspace.display());
            }
            match binaries.dt_main {
                Some(dt_main) => match dt_main_version(&dt_main) {
                    Ok(version) => {
                        println!("dt-main {version}");
                        println!("dt-main path {}", dt_main.display());
                        warn_if_dt_main_version_unsupported_value(&version, &dt_main);
                    }
                    Err(err) => {
                        println!(
                            "dt-main unknown: failed to run '{} --version': {err}",
                            dt_main.display(),
                        );
                    }
                },
                None => println!("dt-main not found under workspace {}", workspace.display()),
            }
        }
        Err(err) => println!("dt-main unknown: failed to read dtscli config: {err}"),
    }
    println!("required ape-dts version: {}", REQUIRED_DTS_VERSION);
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CliConfig {
    workspace: String,
    log_dir: String,
}

impl CliConfig {
    fn default_effective() -> Self {
        let workspace = ".".to_string();
        Self {
            workspace: workspace.clone(),
            log_dir: "./logs".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskMetadata {
    task_name: String,
    mode: String,
    source_db: String,
    target_db: String,
    source_url: String,
    target_url: String,
    task_dir: String,
    config_file: String,
    log_dir: String,
    runtime_log4rs_file: String,
    dt_main: String,
    pid: Option<u32>,
    created_at_unix_secs: u64,
}

#[derive(Debug, Parser)]
#[command(name = "dtscli", version, about = ABOUT)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Manage dtscli defaults.
    Config(ConfigCommand),
    /// Create and start a DTS task.
    Create(CreateArgs),
    /// Start a stopped DTS task.
    Start(StartArgs),
    /// List local DTS tasks.
    List,
    /// Print or follow task logs.
    Logs(LogsArgs),
    /// Show task details.
    Show(ShowArgs),
    /// Stop a running task.
    Stop(StopArgs),
    /// Delete a stopped task record and local task files.
    Delete(DeleteArgs),
    /// Generate shell completion scripts.
    Completion(CompletionArgs),
    /// Print version information.
    Version,
}

#[derive(Debug, Args)]
struct ConfigCommand {
    #[command(subcommand)]
    command: ConfigSubcommand,
}

#[derive(Debug, Subcommand)]
enum ConfigSubcommand {
    /// Show current dtscli defaults.
    Get,
    /// Set dtscli defaults.
    Set(ConfigSetArgs),
}

#[derive(Debug, Args)]
struct ConfigSetArgs {
    #[arg(
        long,
        help = "Workspace directory containing dt-main and dtscli binaries. Relative paths are resolved from the current directory."
    )]
    workspace: Option<String>,
    #[arg(long = "log-dir")]
    log_dir: Option<String>,
}

#[derive(Debug, Args, Default)]
#[command(after_help = CREATE_AFTER_HELP)]
struct CreateArgs {
    #[arg(
        long = "name",
        help = "Required. Task id written to [global].task_id; also used by list/logs/stop. Format: ASCII letter/digit first, then letters/digits/._-, max 128 bytes."
    )]
    task_name: String,
    #[arg(
        short = 'f',
        long = "file",
        value_name = "PATH",
        conflicts_with_all = [
            "mode",
            "preflight",
            "source_url",
            "target_url",
            "source_db",
            "target_db",
            "source_user",
            "source_password",
            "target_user",
            "target_password",
            "filter_do",
            "filter_ignore",
            "do_events",
            "mysql_server_id",
            "pg_slot_name",
            "set"
        ],
        help = "Optional. Start dt-main with an existing task_config.ini. --name is still required for local task tracking."
    )]
    file: Option<String>,
    #[arg(
        long,
        required_unless_present = "file",
        value_enum,
        help = "Required unless --file is provided. Task mode to generate. Allowed: struct, snapshot, cdc."
    )]
    mode: Option<Mode>,
    #[arg(
        long,
        help = "Optional. Run preflight checks for the selected mode without starting the task."
    )]
    preflight: bool,
    #[arg(
        long = "source",
        required_unless_present = "file",
        help = "Required unless --file is provided. Source database URL. Must include a supported scheme prefix: mysql://, postgres://, postgresql://, pg://, mongodb://, mongo://, or redis://. PostgreSQL and MongoDB URLs must include a database when --source-db is omitted."
    )]
    source_url: Option<String>,
    #[arg(
        long = "target",
        required_unless_present = "file",
        help = "Required unless --file is provided. Target database URL. Must include a supported scheme prefix. PostgreSQL and MongoDB URLs must include a database when --target-db is omitted. v1 requires source and target to be the same engine."
    )]
    target_url: Option<String>,
    #[arg(
        long = "source-db",
        value_enum,
        help = "Optional. Source engine override when URL scheme is ambiguous. Usually inferred from --source."
    )]
    source_db: Option<DbType>,
    #[arg(
        long = "target-db",
        value_enum,
        help = "Optional. Target engine override when URL scheme is ambiguous. Usually inferred from --target."
    )]
    target_db: Option<DbType>,
    #[arg(
        long = "source-user",
        help = "Optional. Source username when it is not embedded in --source."
    )]
    source_user: Option<String>,
    #[arg(
        long = "source-password",
        help = "Optional. Source password when it is not embedded in --source."
    )]
    source_password: Option<String>,
    #[arg(
        long = "target-user",
        help = "Optional. Target username when it is not embedded in --target."
    )]
    target_user: Option<String>,
    #[arg(
        long = "target-password",
        help = "Optional. Target password when it is not embedded in --target."
    )]
    target_password: Option<String>,
    #[arg(
        long = "do",
        value_name = "EXPRESSIONS",
        help = "Optional. Databases/schemas or tables/collections to include, comma-separated. Use db or db.table expressions, for example test_db,test_db.orders."
    )]
    filter_do: Option<String>,
    #[arg(
        long = "ignore",
        value_name = "EXPRESSIONS",
        help = "Optional. Databases/schemas or tables/collections to exclude, comma-separated. Use db or db.table expressions. Exclude rules take precedence over include rules."
    )]
    filter_ignore: Option<String>,
    #[arg(
        long = "do-events",
        help = "Optional. CDC events to include, comma-separated. Typical values: insert,update,delete."
    )]
    do_events: Option<String>,
    #[arg(
        long = "mysql-server-id",
        help = "Optional for MySQL CDC. Replication server_id. If omitted, dtscli generates a random value in 10001..=4294836224."
    )]
    mysql_server_id: Option<String>,
    #[arg(
        long = "pg-slot-name",
        help = "Optional for PostgreSQL CDC. Logical replication slot name. If omitted, dtscli derives a valid slot name from --name."
    )]
    pg_slot_name: Option<String>,
    #[arg(
        long = "dry-run",
        help = "Optional. Print the generated task_config.ini only; do not create task files or start dt-main."
    )]
    dry_run: bool,
    #[arg(
        long = "set",
        help = "Optional. Override generated config with section.key=value. Can be repeated, for example --set parallelizer.parallel_size=8."
    )]
    set: Vec<String>,
}

impl CreateArgs {
    fn into_config(self) -> Result<CreateConfig> {
        Ok(CreateConfig {
            task_name: self.task_name,
            mode: self
                .mode
                .context("--mode is required unless --file is provided")?,
            preflight: self.preflight,
            source_url: self
                .source_url
                .context("--source is required unless --file is provided")?,
            target_url: self
                .target_url
                .context("--target is required unless --file is provided")?,
            source_db: self.source_db,
            target_db: self.target_db,
            source_user: self.source_user,
            source_password: self.source_password,
            target_user: self.target_user,
            target_password: self.target_password,
            filter_do: self.filter_do,
            filter_ignore: self.filter_ignore,
            do_events: self.do_events,
            mysql_server_id: self.mysql_server_id,
            pg_slot_name: self.pg_slot_name,
            set: self.set,
        })
    }
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(value_name = "task_name")]
    task_name: String,
    #[arg(short = 'f', long = "follow")]
    follow: bool,
    #[arg(short = 'n', long = "lines", default_value_t = 100)]
    lines: usize,
    #[arg(
        long = "file",
        help = "Optional. Log file to read: default, monitor, commit, position, finished, stdout, stderr. If omitted, dtscli picks default.log when present, otherwise stderr/stdout for startup failures."
    )]
    file: Option<String>,
}

#[derive(Debug, Args)]
struct StartArgs {
    #[arg(value_name = "task_name")]
    task_name: String,
}

#[derive(Debug, Args)]
struct ShowArgs {
    #[arg(value_name = "task_name")]
    task_name: String,
}

#[derive(Debug, Args)]
struct StopArgs {
    #[arg(value_name = "task_name")]
    task_name: String,
    #[arg(long = "timeout", default_value_t = 10)]
    timeout_secs: u64,
    #[arg(long = "force")]
    force: bool,
}

#[derive(Debug, Args)]
struct DeleteArgs {
    #[arg(value_name = "task_name")]
    task_name: String,
    #[arg(
        long = "force",
        help = "Stop the task first if it is still running, then delete local task files."
    )]
    force: bool,
}

#[derive(Debug, Args)]
struct CompletionArgs {
    #[command(subcommand)]
    shell: CompletionShell,
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum CompletionShell {
    /// Generate the autocompletion script for the bash shell.
    #[command(after_help = BASH_COMPLETION_AFTER_HELP)]
    Bash,
    /// Generate the autocompletion script for the fish shell.
    #[command(after_help = FISH_COMPLETION_AFTER_HELP)]
    Fish,
    /// Generate the autocompletion script for the zsh shell.
    #[command(after_help = ZSH_COMPLETION_AFTER_HELP)]
    Zsh,
}

fn handle_config(command: ConfigCommand) -> Result<()> {
    match command.command {
        ConfigSubcommand::Get => {
            let cfg = load_cli_config()?;
            println!("{}", serde_json::to_string_pretty(&cfg)?);
        }
        ConfigSubcommand::Set(args) => {
            let mut cfg = load_cli_config()?;
            if let Some(workspace) = args.workspace {
                cfg.workspace = workspace;
            }
            if let Some(log_dir) = args.log_dir {
                cfg.log_dir = log_dir;
            }
            save_cli_config(&cfg)?;
            println!("{}", serde_json::to_string_pretty(&cfg)?);
            warn_if_workspace_binaries_missing(&cfg);
        }
    }
    Ok(())
}

fn handle_completion(args: CompletionArgs) {
    let mut command = Cli::command();
    match args.shell {
        CompletionShell::Bash => {
            generate(
                clap_complete::Shell::Bash,
                &mut command,
                APP_NAME,
                &mut io::stdout(),
            );
        }
        CompletionShell::Fish => {
            generate(
                clap_complete::Shell::Fish,
                &mut command,
                APP_NAME,
                &mut io::stdout(),
            );
        }
        CompletionShell::Zsh => {
            generate(
                clap_complete::Shell::Zsh,
                &mut command,
                APP_NAME,
                &mut io::stdout(),
            );
        }
    }
}

fn handle_create(create: CreateArgs) -> Result<()> {
    let cfg = load_cli_config()?;
    let task_name = create.task_name.clone();
    validate_task_name(&task_name)?;
    let workspace = resolve_workspace(&cfg)?;

    if let Some(file) = create.file.as_deref() {
        let config_file = resolve_path(file)?;
        let ini = fs::read_to_string(&config_file)
            .with_context(|| format!("failed to read {}", config_file.display()))?;
        if create.dry_run {
            print!("{ini}");
            return Ok(());
        }

        let dt_main = resolve_dt_main(&cfg)?;
        warn_if_dt_main_version_unsupported(&dt_main);
        let details = inspect_task_config(&config_file, &workspace)?;
        if details.preflight {
            return run_preflight(&dt_main, &workspace, &config_file, None);
        }
        return start_persistent_task(
            &task_name,
            &details.mode,
            &details.source_db,
            &details.target_db,
            &details.source_url,
            &details.target_url,
            &workspace,
            &dt_main,
            &config_file,
            None,
            &details.runtime_log_dir,
            &details.runtime_log4rs_file,
        );
    }

    let dry_run = create.dry_run;
    let create = create.into_config()?;
    let task_name = create.task_name.as_str();
    let source_url = create.source_url.as_str();
    let target_url = create.target_url.as_str();
    let source_db = infer_db_type(source_url, create.source_db.clone())?;
    let target_db = infer_db_type(target_url, create.target_db.clone())?;
    if source_db != target_db {
        bail!(
            "v1 only supports same-engine tasks, got source={} target={}",
            source_db.as_config_value(),
            target_db.as_config_value()
        );
    }

    let preflight_dir = create.preflight.then(|| preflight_temp_dir(task_name));
    let runtime_log_dir = preflight_dir
        .as_ref()
        .map(|dir| dir.join("logs"))
        .unwrap_or(resolve_log_dir(&cfg, task_name)?);
    let runtime_log4rs_file = workspace.join("log4rs.yaml");
    let config_file = preflight_dir
        .as_ref()
        .map(|dir| dir.join("task_config.ini"))
        .unwrap_or(task_root()?.join(task_name).join("task_config.ini"));

    let ini = build_task_config(
        &create,
        &source_db,
        &target_db,
        &runtime_log_dir,
        &runtime_log4rs_file,
    )?;

    if dry_run {
        print!("{ini}");
        return Ok(());
    }

    let dt_main = resolve_dt_main(&cfg)?;
    warn_if_dt_main_version_unsupported(&dt_main);
    if let Some(preflight_dir) = preflight_dir {
        fs::create_dir_all(&runtime_log_dir)
            .with_context(|| format!("failed to create log dir {}", runtime_log_dir.display()))?;
        fs::write(&config_file, ini)
            .with_context(|| format!("failed to write {}", config_file.display()))?;
        return run_preflight(&dt_main, &workspace, &config_file, Some(&preflight_dir));
    }

    start_persistent_task(
        task_name,
        create.mode.as_str(),
        source_db.as_config_value(),
        target_db.as_config_value(),
        source_url,
        target_url,
        &workspace,
        &dt_main,
        &config_file,
        Some(&ini),
        &runtime_log_dir,
        &runtime_log4rs_file,
    )
}

#[derive(Debug)]
struct TaskConfigDetails {
    mode: String,
    source_db: String,
    target_db: String,
    source_url: String,
    target_url: String,
    runtime_log_dir: PathBuf,
    runtime_log4rs_file: PathBuf,
    preflight: bool,
}

fn inspect_task_config(config_file: &Path, workspace: &Path) -> Result<TaskConfigDetails> {
    let mut ini = Ini::new();
    ini.load(config_file.display().to_string())
        .map_err(|err| anyhow!("failed to parse {}: {err}", config_file.display()))?;

    let runtime_log_dir = resolve_workspace_path(
        workspace,
        ini.get("runtime", "log_dir").as_deref().unwrap_or("./logs"),
    );
    let runtime_log4rs_file = resolve_workspace_path(
        workspace,
        ini.get("runtime", "log4rs_file")
            .as_deref()
            .unwrap_or("./log4rs.yaml"),
    );
    let preflight =
        ini.get("precheck", "do_struct_init").is_some() && ini.get("precheck", "do_cdc").is_some();

    Ok(TaskConfigDetails {
        mode: ini
            .get("extractor", "extract_type")
            .unwrap_or_else(|| "file".to_string()),
        source_db: ini
            .get("extractor", "db_type")
            .unwrap_or_else(|| "-".to_string()),
        target_db: ini
            .get("sinker", "db_type")
            .unwrap_or_else(|| "-".to_string()),
        source_url: ini.get("extractor", "url").unwrap_or_default(),
        target_url: ini.get("sinker", "url").unwrap_or_default(),
        runtime_log_dir,
        runtime_log4rs_file,
        preflight,
    })
}

#[allow(clippy::too_many_arguments)]
fn start_persistent_task(
    task_name: &str,
    mode: &str,
    source_db: &str,
    target_db: &str,
    source_url: &str,
    target_url: &str,
    workspace: &Path,
    dt_main: &Path,
    config_file: &Path,
    config_content: Option<&str>,
    runtime_log_dir: &Path,
    runtime_log4rs_file: &Path,
) -> Result<()> {
    let task_dir = task_root()?.join(task_name);
    if task_dir.exists() {
        bail!(
            "task '{task_name}' already exists at {}",
            task_dir.display()
        );
    }
    fs::create_dir_all(&task_dir)
        .with_context(|| format!("failed to create task dir {}", task_dir.display()))?;
    if let Some(content) = config_content {
        fs::write(config_file, content)
            .with_context(|| format!("failed to write {}", config_file.display()))?;
    }

    let mut metadata = TaskMetadata {
        task_name: task_name.to_string(),
        mode: mode.to_string(),
        source_db: source_db.to_string(),
        target_db: target_db.to_string(),
        source_url: source_url.to_string(),
        target_url: target_url.to_string(),
        task_dir: task_dir.display().to_string(),
        config_file: config_file.display().to_string(),
        log_dir: runtime_log_dir.display().to_string(),
        runtime_log4rs_file: runtime_log4rs_file.display().to_string(),
        dt_main: dt_main.display().to_string(),
        pid: None,
        created_at_unix_secs: unix_secs(),
    };
    launch_persistent_task(&task_dir, workspace, dt_main, &mut metadata)
}

fn restart_persistent_task(
    task_dir: &Path,
    workspace: &Path,
    dt_main: &Path,
    metadata: &mut TaskMetadata,
) -> Result<()> {
    reject_if_task_running(task_dir, metadata)?;
    launch_persistent_task(task_dir, workspace, dt_main, metadata)
}

fn reject_if_task_running(task_dir: &Path, metadata: &TaskMetadata) -> Result<()> {
    let mut pids = Vec::new();
    if let Some(pid) = read_pid(task_dir) {
        pids.push(pid);
    }
    if let Some(pid) = metadata.pid {
        if !pids.contains(&pid) {
            pids.push(pid);
        }
    }
    if let Some(pid) = pids.into_iter().find(|pid| process_exists(*pid)) {
        bail!(
            "task '{}' is already running with pid {}",
            metadata.task_name,
            pid
        );
    }
    Ok(())
}

fn launch_persistent_task(
    task_dir: &Path,
    workspace: &Path,
    dt_main: &Path,
    metadata: &mut TaskMetadata,
) -> Result<()> {
    let config_file = PathBuf::from(&metadata.config_file);
    let runtime_log_dir = PathBuf::from(&metadata.log_dir);
    let runtime_log4rs_file = PathBuf::from(&metadata.runtime_log4rs_file);
    fs::create_dir_all(&runtime_log_dir)
        .with_context(|| format!("failed to create log dir {}", runtime_log_dir.display()))?;
    warn_if_runtime_log4rs_missing(&runtime_log4rs_file);

    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(task_dir.join("stdout.log"))?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(task_dir.join("stderr.log"))?;
    let mut child = Command::new(dt_main)
        .arg(&config_file)
        .current_dir(workspace)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .with_context(|| format!("failed to start {}", dt_main.display()))?;
    let pid = child.id();
    fs::write(task_dir.join("pid"), pid.to_string())?;

    metadata.dt_main = dt_main.display().to_string();
    metadata.pid = Some(pid);
    fs::write(
        task_dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata)?,
    )?;

    report_task_start(
        &metadata.task_name,
        pid,
        &config_file,
        &runtime_log_dir,
        &mut child,
    )
}

fn warn_if_runtime_log4rs_missing(runtime_log4rs_file: &Path) {
    if !runtime_log4rs_file.is_file() {
        eprintln!(
            "warning: log4rs config not found at {}; default.log will not be created; stderr/stdout are fallback logs",
            runtime_log4rs_file.display()
        );
    }
}

fn run_preflight(
    dt_main: &Path,
    workspace: &Path,
    config_file: &Path,
    temp_dir: Option<&Path>,
) -> Result<()> {
    println!(
        "running preflight with config={}; streaming output until precheck exits (Ctrl-C to stop)",
        config_file.display()
    );
    PREFLIGHT_INTERRUPTED.store(false, Ordering::SeqCst);
    let _signal_guard = PreflightSignalGuard::install();
    let result = Command::new(dt_main)
        .arg(config_file)
        .current_dir(workspace)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("failed to start {}", dt_main.display()))
        .and_then(|mut child| wait_for_preflight(&mut child));

    if let Some(temp_dir) = temp_dir {
        if let Err(err) = fs::remove_dir_all(temp_dir) {
            eprintln!(
                "warning: failed to remove preflight temp dir {}: {err}",
                temp_dir.display()
            );
        }
    }

    let status = result?;
    if PREFLIGHT_INTERRUPTED.load(Ordering::SeqCst) {
        bail!("preflight interrupted");
    }
    if !status.success() {
        bail!("preflight failed with status {status}");
    }
    println!("preflight finished successfully");
    Ok(())
}

fn wait_for_preflight(child: &mut std::process::Child) -> Result<std::process::ExitStatus> {
    let mut signal_forwarded = false;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if PREFLIGHT_INTERRUPTED.load(Ordering::SeqCst) && !signal_forwarded {
            let _ = send_signal(child.id(), "INT");
            signal_forwarded = true;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

struct PreflightSignalGuard {
    previous: libc::sighandler_t,
}

impl PreflightSignalGuard {
    fn install() -> Self {
        let previous = unsafe {
            libc::signal(
                libc::SIGINT,
                handle_preflight_interrupt as libc::sighandler_t,
            )
        };
        Self { previous }
    }
}

impl Drop for PreflightSignalGuard {
    fn drop(&mut self) {
        unsafe {
            libc::signal(libc::SIGINT, self.previous);
        }
    }
}

extern "C" fn handle_preflight_interrupt(_: libc::c_int) {
    PREFLIGHT_INTERRUPTED.store(true, Ordering::SeqCst);
}

fn preflight_temp_dir(task_name: &str) -> PathBuf {
    env::temp_dir().join(format!(
        "dtscli-preflight-{task_name}-{}-{}",
        std::process::id(),
        unix_nanos()
    ))
}

fn handle_list() -> Result<()> {
    let root = task_root()?;
    println!(
        "{:<24} {:<10} {:<8} {:<10} {}",
        "TASK", "STATUS", "PID", "MODE", "LOG_DIR"
    );
    if !root.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let task_dir = entry.path();
        let metadata = read_metadata(&task_dir).ok();
        let task_name = metadata
            .as_ref()
            .map(|m| m.task_name.clone())
            .unwrap_or_else(|| entry.file_name().to_string_lossy().to_string());
        let pid = read_pid(&task_dir);
        let status = pid
            .filter(|pid| process_exists(*pid))
            .map(|_| "Running")
            .unwrap_or("Stopped");
        let pid_text = pid
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let mode = metadata.as_ref().map(|m| m.mode.as_str()).unwrap_or("-");
        let log_dir = metadata.as_ref().map(|m| m.log_dir.as_str()).unwrap_or("-");
        println!(
            "{:<24} {:<10} {:<8} {:<10} {}",
            task_name, status, pid_text, mode, log_dir
        );
    }
    Ok(())
}

fn handle_start(start: StartArgs) -> Result<()> {
    validate_task_name(&start.task_name)?;
    let task_dir = existing_task_dir(&start.task_name)?;
    let mut metadata = read_metadata(&task_dir)?;

    let cfg = load_cli_config()?;
    let workspace = resolve_workspace(&cfg)?;
    let dt_main = resolve_dt_main(&cfg)?;
    warn_if_dt_main_version_unsupported(&dt_main);
    restart_persistent_task(&task_dir, &workspace, &dt_main, &mut metadata)
}

fn handle_show(args: ShowArgs) -> Result<()> {
    let task_dir = existing_task_dir(&args.task_name)?;
    let metadata = read_metadata(&task_dir)?;
    println!("{}", serde_json::to_string_pretty(&metadata)?);
    Ok(())
}

fn handle_logs(logs: LogsArgs) -> Result<()> {
    let task_dir = task_root()?.join(&logs.task_name);
    let metadata = read_metadata(&task_dir)?;
    let log_file = match logs.file.as_deref() {
        Some(file) => resolve_log_file(&metadata, file)?,
        None => select_default_log_file(&metadata)?,
    };

    let printed = print_tail(&log_file, logs.lines)?;
    if !printed {
        println!(
            "no log output found for task '{}' in {}",
            logs.task_name,
            log_file.display()
        );
    }
    if logs.follow {
        follow_file(&log_file)?;
    }
    Ok(())
}

fn handle_stop(stop: StopArgs) -> Result<()> {
    let task_dir = task_root()?.join(&stop.task_name);
    let pid = read_pid(&task_dir)
        .ok_or_else(|| anyhow!("pid not found for task '{}'", stop.task_name))?;
    if !process_exists(pid) {
        println!("task '{}' is already stopped", stop.task_name);
        return Ok(());
    }

    stop_pid(&stop.task_name, pid, stop.timeout_secs, stop.force)?;
    println!("task '{}' stopped", stop.task_name);
    Ok(())
}

fn handle_delete(delete: DeleteArgs) -> Result<()> {
    let task_dir = existing_task_dir(&delete.task_name)?;

    if let Some(pid) = read_pid(&task_dir) {
        if process_exists(pid) {
            if !delete.force {
                bail!(
                    "task '{}' is still running with pid {}; stop it first or use --force",
                    delete.task_name,
                    pid
                );
            }
        }
    }

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    if !confirm_delete(&delete.task_name, &mut stdin.lock(), &mut stdout)? {
        println!("delete cancelled");
        return Ok(());
    }

    if let Some(pid) = read_pid(&task_dir) {
        if process_exists(pid) {
            stop_pid(&delete.task_name, pid, 10, true)?;
        }
    }

    let metadata = match read_metadata(&task_dir) {
        Ok(metadata) => Some(metadata),
        Err(err) => {
            eprintln!("warning: runtime logs will not be deleted because metadata could not be read: {err}");
            None
        }
    };
    delete_task_files(&task_dir, metadata.as_ref())?;
    println!("task '{}' deleted", delete.task_name);
    Ok(())
}

fn delete_task_files(task_dir: &Path, metadata: Option<&TaskMetadata>) -> Result<()> {
    if let Some(metadata) = metadata {
        delete_runtime_log_dir(metadata)?;
    }

    if task_dir.exists() {
        fs::remove_dir_all(task_dir)
            .with_context(|| format!("failed to delete {}", task_dir.display()))?;
    }
    Ok(())
}

fn delete_runtime_log_dir(metadata: &TaskMetadata) -> Result<()> {
    let log_dir = PathBuf::from(&metadata.log_dir);
    if !log_dir.exists() {
        return Ok(());
    }
    if log_dir.file_name().and_then(|name| name.to_str()) != Some(metadata.task_name.as_str()) {
        eprintln!(
            "warning: skipped deleting runtime log dir {} because it is not scoped to task '{}'",
            log_dir.display(),
            metadata.task_name
        );
        return Ok(());
    }
    fs::remove_dir_all(&log_dir)
        .with_context(|| format!("failed to delete runtime log dir {}", log_dir.display()))?;
    Ok(())
}

fn validate_task_name(task_name: &str) -> Result<()> {
    let mut chars = task_name.chars();
    let Some(first) = chars.next() else {
        bail!("task_name must not be empty");
    };
    if task_name.len() > 128 {
        bail!("task_name must be at most 128 bytes");
    }
    if !first.is_ascii_alphanumeric() {
        bail!("task_name must start with an ASCII letter or digit");
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')) {
        bail!("task_name can only contain ASCII letters, digits, '_', '-', and '.'");
    }
    Ok(())
}

fn load_cli_config() -> Result<CliConfig> {
    let path = cli_config_path()?;
    if !path.exists() {
        return Ok(CliConfig::default_effective());
    }
    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    Ok(serde_json::from_str(&content)?)
}

fn save_cli_config(cfg: &CliConfig) -> Result<()> {
    let path = cli_config_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&path, serde_json::to_string_pretty(cfg)?)?;
    Ok(())
}

fn cli_home() -> Result<PathBuf> {
    if let Ok(home) = env::var("APE_DTS_HOME") {
        return Ok(PathBuf::from(home));
    }
    let home = env::var("HOME").context("HOME is not set and APE_DTS_HOME is not configured")?;
    Ok(PathBuf::from(home).join(".ape-dts"))
}

fn cli_config_path() -> Result<PathBuf> {
    Ok(cli_home()?.join("config.json"))
}

fn task_root() -> Result<PathBuf> {
    Ok(cli_home()?.join("tasks"))
}

fn existing_task_dir(task_name: &str) -> Result<PathBuf> {
    let task_dir = task_root()?.join(task_name);
    if !task_dir.is_dir() {
        bail!("task '{task_name}' does not exist");
    }
    Ok(task_dir)
}

fn confirm_delete(
    task_name: &str,
    input: &mut impl BufRead,
    output: &mut impl Write,
) -> Result<bool> {
    loop {
        write!(
            output,
            "type task name '{}' to confirm deletion: ",
            task_name
        )?;
        output.flush()?;

        let mut confirmation = String::new();
        if input.read_line(&mut confirmation)? == 0 {
            return Ok(false);
        }
        let confirmation = confirmation.trim_end_matches(['\r', '\n']);
        if confirmation == task_name {
            return Ok(true);
        }
        writeln!(
            output,
            ">> typed \"{}\" does not match \"{}\"",
            confirmation, task_name
        )?;
    }
}

fn resolve_log_dir(cfg: &CliConfig, task_name: &str) -> Result<PathBuf> {
    let log_dir = PathBuf::from(&cfg.log_dir);
    let base = if log_dir.is_absolute() {
        log_dir
    } else {
        resolve_workspace(cfg)?.join(log_dir)
    };
    Ok(clean_path(base.join(task_name)))
}

fn resolve_workspace(cfg: &CliConfig) -> Result<PathBuf> {
    resolve_path(&cfg.workspace)
}

fn resolve_path(path: &str) -> Result<PathBuf> {
    let raw = PathBuf::from(path);
    if raw.is_absolute() {
        Ok(clean_path(raw))
    } else {
        Ok(clean_path(env::current_dir()?.join(raw)))
    }
}

fn resolve_workspace_path(workspace: &Path, path: &str) -> PathBuf {
    let raw = PathBuf::from(path);
    if raw.is_absolute() {
        clean_path(raw)
    } else {
        clean_path(workspace.join(raw))
    }
}

fn clean_path(path: PathBuf) -> PathBuf {
    let mut cleaned = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                cleaned.pop();
            }
            _ => cleaned.push(component.as_os_str()),
        }
    }
    cleaned
}

#[derive(Debug)]
struct WorkspaceBinaries {
    cli: Option<PathBuf>,
    dt_main: Option<PathBuf>,
}

fn resolve_workspace_binaries(workspace: &Path) -> WorkspaceBinaries {
    WorkspaceBinaries {
        cli: find_workspace_binary(workspace, &["dtscli"]),
        dt_main: find_workspace_binary(workspace, &["dt-main"]),
    }
}

fn find_workspace_binary(workspace: &Path, names: &[&str]) -> Option<PathBuf> {
    for dir in [workspace.to_path_buf(), workspace.join("bin")] {
        for name in names {
            let path = dir.join(name);
            if path.is_file() {
                return Some(path);
            }
        }
    }
    None
}

fn command_should_warn_workspace_binaries(command: &Commands) -> bool {
    matches!(command, Commands::Create(_) | Commands::Start(_))
}

fn warn_if_workspace_binaries_missing_from_config() {
    match load_cli_config() {
        Ok(cfg) => warn_if_workspace_binaries_missing(&cfg),
        Err(err) => eprintln!("warning: failed to read dtscli config: {err}"),
    }
}

fn warn_if_workspace_binaries_missing(cfg: &CliConfig) {
    match resolve_workspace(cfg) {
        Ok(workspace) => {
            let binaries = resolve_workspace_binaries(&workspace);
            if binaries.cli.is_none() {
                eprintln!(
                    "warning: dtscli binary not found under workspace {}; expected ./dtscli or ./bin/dtscli",
                    workspace.display()
                );
            }
            if binaries.dt_main.is_none() {
                eprintln!(
                    "warning: dt-main binary not found under workspace {}; create/start commands will fail until ./dt-main or ./bin/dt-main exists",
                    workspace.display()
                );
            }
        }
        Err(err) => eprintln!("warning: failed to resolve workspace: {err}"),
    }
}

fn resolve_dt_main(cfg: &CliConfig) -> Result<PathBuf> {
    let workspace = resolve_workspace(cfg)?;
    if let Some(dt_main) = resolve_workspace_binaries(&workspace).dt_main {
        return Ok(dt_main);
    }
    bail!(
        "dt-main binary not found under workspace {}; put dt-main at ./dt-main or ./bin/dt-main, or run `dtscli config set --workspace /path/to/release-dir`",
        workspace.display()
    )
}

fn dt_main_version(dt_main: &Path) -> Result<String> {
    let output = Command::new(dt_main)
        .arg("--version")
        .output()
        .with_context(|| format!("failed to run '{} --version'", dt_main.display()))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("{}", stderr.trim());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let version = stdout
        .trim()
        .strip_prefix("dt-main")
        .unwrap_or(stdout.trim())
        .trim();
    if version.is_empty() {
        bail!("empty version output");
    }
    Ok(version.to_string())
}

fn warn_if_dt_main_version_unsupported(dt_main: &Path) {
    match dt_main_version(dt_main) {
        Ok(version) => warn_if_dt_main_version_unsupported_value(&version, dt_main),
        Err(err) => eprintln!(
            "warning: failed to check dt-main version at {}: {err}",
            dt_main.display()
        ),
    }
}

fn warn_if_dt_main_version_unsupported_value(version: &str, dt_main: &Path) {
    match version_is_less_than_required(version, REQUIRED_DTS_VERSION) {
        Some(true) => eprintln!(
            "warning: dt-main version {version} at {} is lower than required {REQUIRED_DTS_VERSION}",
            dt_main.display()
        ),
        Some(false) => {}
        None => eprintln!(
            "warning: failed to compare dt-main version {version} with required {REQUIRED_DTS_VERSION}"
        ),
    }
}

fn version_is_less_than_required(version: &str, required: &str) -> Option<bool> {
    Some(parse_version(version)? < parse_version(required)?)
}

fn parse_version(version: &str) -> Option<[u64; 3]> {
    let mut parsed = [0_u64; 3];
    let mut saw_part = false;
    for (index, part) in version
        .trim()
        .trim_start_matches('v')
        .split('.')
        .take(3)
        .enumerate()
    {
        let digits: String = part.chars().take_while(|ch| ch.is_ascii_digit()).collect();
        if digits.is_empty() {
            return None;
        }
        parsed[index] = digits.parse().ok()?;
        saw_part = true;
    }
    saw_part.then_some(parsed)
}

fn read_metadata(task_dir: &Path) -> Result<TaskMetadata> {
    let content = fs::read_to_string(task_dir.join("metadata.json")).with_context(|| {
        format!(
            "failed to read {}",
            task_dir.join("metadata.json").display()
        )
    })?;
    Ok(serde_json::from_str(&content)?)
}

fn read_pid(task_dir: &Path) -> Option<u32> {
    fs::read_to_string(task_dir.join("pid"))
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
}

fn report_task_start(
    task_name: &str,
    pid: u32,
    config_file: &Path,
    runtime_log_dir: &Path,
    child: &mut std::process::Child,
) -> Result<()> {
    std::thread::sleep(std::time::Duration::from_millis(500));
    if let Some(status) = child
        .try_wait()
        .with_context(|| format!("failed to check task process {pid}"))?
    {
        if status.success() {
            println!(
                "task '{}' completed quickly, pid={}, status={}, config={}",
                task_name,
                pid,
                status,
                config_file.display()
            );
        } else {
            println!(
                "task '{}' exited immediately, pid={}, status={}, config={}",
                task_name,
                pid,
                status,
                config_file.display()
            );
        }
        println!("check logs with:");
        println!("  dtscli logs {task_name}");
        println!(
            "runtime logs normally use {}; stderr/stdout are fallbacks for early startup failures",
            runtime_log_dir.join("default.log").display()
        );
        return Ok(());
    }

    println!(
        "task '{}' started, pid={}, config={}",
        task_name,
        pid,
        config_file.display()
    );
    Ok(())
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn send_signal(pid: u32, signal: &str) -> Result<()> {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .status()
        .with_context(|| format!("failed to send SIG{signal} to pid {pid}"))?;
    if !status.success() {
        bail!("failed to send SIG{signal} to pid {pid}");
    }
    Ok(())
}

fn stop_pid(task_name: &str, pid: u32, timeout_secs: u64, force: bool) -> Result<()> {
    send_signal(pid, "INT")?;
    wait_until_stopped(pid, timeout_secs);
    if process_exists(pid) {
        send_signal(pid, "TERM")?;
        wait_until_stopped(pid, timeout_secs);
    }
    if force && process_exists(pid) {
        send_signal(pid, "KILL")?;
        wait_until_stopped(pid, 2);
    }

    if process_exists(pid) {
        bail!("task '{}' is still running with pid {}", task_name, pid);
    }
    Ok(())
}

fn wait_until_stopped(pid: u32, timeout_secs: u64) {
    let deadline = unix_secs() + timeout_secs;
    while unix_secs() <= deadline {
        if !process_exists(pid) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

fn print_tail(path: &Path, lines: usize) -> Result<bool> {
    if !path.exists() {
        bail!("log file does not exist: {}", path.display());
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut ring = Vec::new();
    for line in reader.lines() {
        ring.push(line?);
        if ring.len() > lines {
            ring.remove(0);
        }
    }
    let printed = !ring.is_empty();
    for line in ring {
        println!("{line}");
    }
    Ok(printed)
}

fn resolve_log_file(metadata: &TaskMetadata, file: &str) -> Result<PathBuf> {
    match file {
        "stdout" => Ok(PathBuf::from(&metadata.task_dir).join("stdout.log")),
        "stderr" => Ok(PathBuf::from(&metadata.task_dir).join("stderr.log")),
        "default" | "monitor" | "commit" | "position" | "finished" => {
            Ok(PathBuf::from(&metadata.log_dir).join(format!("{file}.log")))
        }
        other => bail!("unsupported log file '{other}'"),
    }
}

fn select_default_log_file(metadata: &TaskMetadata) -> Result<PathBuf> {
    let candidates = [
        resolve_log_file(metadata, "default")?,
        resolve_log_file(metadata, "stderr")?,
        resolve_log_file(metadata, "stdout")?,
    ];

    for path in candidates.iter() {
        if path_has_content(path) {
            return Ok(path.clone());
        }
    }
    for path in candidates.iter() {
        if path.exists() {
            return Ok(path.clone());
        }
    }

    bail!(
        "no log file found for task '{}'. Checked: {}",
        metadata.task_name,
        candidates
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn path_has_content(path: &Path) -> bool {
    path.metadata().map(|meta| meta.len() > 0).unwrap_or(false)
}

fn follow_file(path: &Path) -> Result<()> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    file.seek(SeekFrom::End(0))?;
    loop {
        let mut buf = String::new();
        let bytes = file.read_to_string(&mut buf)?;
        if bytes > 0 {
            print!("{buf}");
            io::stdout().flush()?;
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metadata(task_dir: &Path, config_file: &Path, log_dir: &Path) -> TaskMetadata {
        TaskMetadata {
            task_name: "order_sync".to_string(),
            mode: "snapshot".to_string(),
            source_db: "mysql".to_string(),
            target_db: "mysql".to_string(),
            source_url: "mysql://src:3306".to_string(),
            target_url: "mysql://dst:3307".to_string(),
            task_dir: task_dir.display().to_string(),
            config_file: config_file.display().to_string(),
            log_dir: log_dir.display().to_string(),
            runtime_log4rs_file: task_dir.join("log4rs.yaml").display().to_string(),
            dt_main: "/old/dt-main".to_string(),
            pid: None,
            created_at_unix_secs: 1,
        }
    }

    #[test]
    fn validates_task_name() {
        validate_task_name("order_sync-1.2").unwrap();
        assert!(validate_task_name("_bad").is_err());
        assert!(validate_task_name("bad/name").is_err());
    }

    #[test]
    fn start_args_accept_task_name() {
        let cli = Cli::try_parse_from([APP_NAME, "start", "order_sync"]).unwrap();
        let Commands::Start(args) = cli.command else {
            panic!("expected start command");
        };
        assert_eq!(args.task_name, "order_sync");
    }

    #[test]
    fn restart_rejects_running_persisted_pid() {
        let root = env::temp_dir().join(format!("dtscli-restart-running-test-{}", unix_nanos()));
        let task_dir = root.join("tasks/order_sync");
        fs::create_dir_all(&task_dir).unwrap();
        fs::write(task_dir.join("pid"), std::process::id().to_string()).unwrap();
        let mut metadata = test_metadata(
            &task_dir,
            &task_dir.join("task_config.ini"),
            &root.join("logs"),
        );

        let err = restart_persistent_task(&task_dir, &root, Path::new("/bin/sh"), &mut metadata)
            .unwrap_err();
        assert!(err.to_string().contains("already running with pid"));

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restart_uses_persisted_config_and_updates_pid() {
        let root = env::temp_dir().join(format!("dtscli-restart-test-{}", unix_nanos()));
        let task_dir = root.join("tasks/order_sync");
        let config_file = task_dir.join("task_config.ini");
        let log_dir = root.join("logs/order_sync");
        let marker = root.join("restarted");
        fs::create_dir_all(&task_dir).unwrap();
        fs::write(
            &config_file,
            format!("echo restarted > '{}'\n", marker.display()),
        )
        .unwrap();
        let mut metadata = test_metadata(&task_dir, &config_file, &log_dir);
        fs::write(
            task_dir.join("metadata.json"),
            serde_json::to_string_pretty(&metadata).unwrap(),
        )
        .unwrap();

        restart_persistent_task(&task_dir, &root, Path::new("/bin/sh"), &mut metadata).unwrap();

        let pid = read_pid(&task_dir).unwrap();
        let saved = read_metadata(&task_dir).unwrap();
        assert!(marker.is_file());
        assert_eq!(saved.config_file, config_file.display().to_string());
        assert_eq!(saved.pid, Some(pid));
        assert_eq!(saved.dt_main, "/bin/sh");
        assert_eq!(saved.created_at_unix_secs, 1);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn delete_confirmation_retries_until_exact_task_name() {
        let mut output = Vec::new();
        assert!(confirm_delete("order_sync", &mut "order_sync\n".as_bytes(), &mut output).unwrap());

        output.clear();
        assert!(confirm_delete(
            "order_sync",
            &mut "yes\n order_sync \norder_sync\n".as_bytes(),
            &mut output
        )
        .unwrap());
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains(r#">> typed "yes" does not match "order_sync""#));
        assert!(output.contains(r#">> typed " order_sync " does not match "order_sync""#));
    }

    #[test]
    fn delete_confirmation_cancels_on_eof() {
        let mut output = Vec::new();
        assert!(!confirm_delete("order_sync", &mut "".as_bytes(), &mut output).unwrap());
    }

    #[test]
    fn delete_task_files_removes_task_dir_and_scoped_runtime_logs() {
        let root = env::temp_dir().join(format!("dtscli-delete-test-{}", unix_nanos()));
        let task_dir = root.join("tasks/order_sync");
        let log_dir = root.join("logs/order_sync");
        fs::create_dir_all(&task_dir).unwrap();
        fs::create_dir_all(&log_dir).unwrap();
        fs::write(task_dir.join("metadata.json"), "{}").unwrap();
        fs::write(log_dir.join("default.log"), "runtime log").unwrap();
        let metadata = test_metadata(&task_dir, &task_dir.join("task_config.ini"), &log_dir);

        delete_task_files(&task_dir, Some(&metadata)).unwrap();

        assert!(!task_dir.exists());
        assert!(!log_dir.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn delete_task_files_preserves_unscoped_runtime_log_dir() {
        let root = env::temp_dir().join(format!("dtscli-delete-shared-log-test-{}", unix_nanos()));
        let task_dir = root.join("tasks/order_sync");
        let log_dir = root.join("logs");
        fs::create_dir_all(&task_dir).unwrap();
        fs::create_dir_all(&log_dir).unwrap();
        fs::write(log_dir.join("default.log"), "shared runtime log").unwrap();
        let metadata = test_metadata(&task_dir, &task_dir.join("task_config.ini"), &log_dir);

        delete_task_files(&task_dir, Some(&metadata)).unwrap();

        assert!(!task_dir.exists());
        assert!(log_dir.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn clap_errors_do_not_start_with_error_prefix() {
        let err = Cli::try_parse_from([APP_NAME, "logs"]).unwrap_err();
        let message = clap_message(&err);
        assert!(!message.starts_with("error:"));
        assert!(message.contains("the following required arguments were not provided"));
    }

    #[test]
    fn non_missing_clap_errors_keep_error_prefix() {
        let err = Cli::try_parse_from([APP_NAME, "unknown"]).unwrap_err();
        assert!(err.to_string().starts_with("error:"));
    }

    #[test]
    fn completion_help_is_specific_to_each_shell() {
        let bash_help = Cli::try_parse_from([APP_NAME, "completion", "bash", "--help"])
            .unwrap_err()
            .to_string();
        assert!(bash_help.contains("source <(dtscli completion bash)"));
        assert!(!bash_help.contains("dtscli completion fish | source"));

        let fish_help = Cli::try_parse_from([APP_NAME, "completion", "fish", "--help"])
            .unwrap_err()
            .to_string();
        assert!(fish_help.contains("dtscli completion fish | source"));
        assert!(!fish_help.contains("source <(dtscli completion zsh)"));

        let zsh_help = Cli::try_parse_from([APP_NAME, "completion", "zsh", "--help"])
            .unwrap_err()
            .to_string();
        assert!(zsh_help.contains("source <(dtscli completion zsh)"));
        assert!(!zsh_help.contains("autoload -Uz compinit"));
    }

    #[test]
    fn create_args_accept_only_combined_filter_flags() {
        let cli = Cli::try_parse_from([
            APP_NAME,
            "create",
            "--name",
            "order_sync",
            "--mode",
            "snapshot",
            "--source",
            "mysql://src:3306",
            "--target",
            "mysql://dst:3307",
            "--do",
            "test_db,`heh.e`.`ta,ble`",
            "--ignore",
            "test_db.tmp_*",
        ])
        .unwrap();

        let Commands::Create(args) = cli.command else {
            panic!("expected create command");
        };
        assert_eq!(args.filter_do.as_deref(), Some("test_db,`heh.e`.`ta,ble`"));
        assert_eq!(args.filter_ignore.as_deref(), Some("test_db.tmp_*"));

        for flag in ["--do-dbs", "--do-tbs", "--ignore-dbs", "--ignore-tbs"] {
            let err = Cli::try_parse_from([
                APP_NAME,
                "create",
                "--name",
                "order_sync",
                "--mode",
                "snapshot",
                "--source",
                "mysql://src:3306",
                "--target",
                "mysql://dst:3307",
                flag,
                "test_db",
            ])
            .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn create_args_accept_file_without_generated_config_flags() {
        let cli = Cli::try_parse_from([
            APP_NAME,
            "create",
            "--name",
            "order_sync",
            "--file",
            "task.ini",
        ])
        .unwrap();

        let Commands::Create(args) = cli.command else {
            panic!("expected create command");
        };
        assert_eq!(args.task_name, "order_sync");
        assert_eq!(args.file.as_deref(), Some("task.ini"));
        assert!(args.mode.is_none());
        assert!(args.source_url.is_none());
        assert!(args.target_url.is_none());
    }

    #[test]
    fn create_file_conflicts_with_generated_config_flags() {
        for args in [
            vec!["--mode", "snapshot"],
            vec!["--source", "mysql://src:3306"],
            vec!["--do", "test_db"],
            vec!["--set", "runtime.log_level=debug"],
            vec!["--preflight"],
        ] {
            let err = Cli::try_parse_from(
                [
                    APP_NAME,
                    "create",
                    "--name",
                    "order_sync",
                    "--file",
                    "task.ini",
                ]
                .into_iter()
                .chain(args),
            )
            .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
        }
    }

    #[test]
    fn inspect_task_config_resolves_runtime_paths_and_detects_preflight() {
        let root = env::temp_dir().join(format!("dtscli-config-test-{}", unix_nanos()));
        let workspace = root.join("workspace");
        let config_file = root.join("task_config.ini");
        fs::create_dir_all(&workspace).unwrap();
        fs::write(
            &config_file,
            r#"[extractor]
db_type=mysql
extract_type=struct
url=mysql://src:3306

[sinker]
db_type=mysql
url=mysql://dst:3307

[runtime]
log_dir=./logs/order_sync
log4rs_file=./log4rs.yaml

[precheck]
do_struct_init=true
do_cdc=false
"#,
        )
        .unwrap();

        let details = inspect_task_config(&config_file, &workspace).unwrap();
        assert_eq!(details.mode, "struct");
        assert_eq!(details.source_db, "mysql");
        assert_eq!(details.target_db, "mysql");
        assert_eq!(details.runtime_log_dir, workspace.join("logs/order_sync"));
        assert_eq!(details.runtime_log4rs_file, workspace.join("log4rs.yaml"));
        assert!(details.preflight);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cli_config_ignores_legacy_dt_main_field() {
        let cfg: CliConfig = serde_json::from_str(
            r#"{
                "workspace": "/tmp/ape-dts",
                "log_dir": "./logs",
                "dt_main": "/tmp/ape-dts/dt-main"
            }"#,
        )
        .unwrap();

        assert_eq!(cfg.workspace, "/tmp/ape-dts");
        assert_eq!(cfg.log_dir, "./logs");
    }

    #[test]
    fn finds_workspace_binaries_in_root_or_bin_dir() {
        let root = env::temp_dir().join(format!("dtscli-test-{}", unix_secs()));
        let bin = root.join("bin");
        fs::create_dir_all(&bin).unwrap();
        fs::write(root.join("dt-main"), "").unwrap();
        fs::write(bin.join("dtscli"), "").unwrap();

        let binaries = resolve_workspace_binaries(&root);
        assert_eq!(binaries.dt_main.unwrap(), root.join("dt-main"));
        assert_eq!(binaries.cli.unwrap(), bin.join("dtscli"));

        fs::remove_dir_all(root).unwrap();
    }
}
