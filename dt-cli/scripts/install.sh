#!/usr/bin/env sh
set -eu

INSTALL_DIR=${INSTALL_DIR:-"$HOME/.local/bin"}
UPDATE_PATH=${UPDATE_PATH:-1}

mkdir -p "$INSTALL_DIR"

if [ -f "./dtscli" ]; then
  DTS_CLI_BIN="./dtscli"
elif [ "$#" -ge 1 ]; then
  DTS_CLI_BIN=$1
else
  echo "dtscli binary not found in current directory." >&2
  echo "usage: sh install.sh /path/to/dtscli" >&2
  exit 1
fi

if [ ! -f "$DTS_CLI_BIN" ]; then
  echo "dtscli binary not found at: $DTS_CLI_BIN" >&2
  exit 1
fi

cp "$DTS_CLI_BIN" "$INSTALL_DIR/dtscli"

chmod +x "$INSTALL_DIR/dtscli"

echo "installed dtscli:"
echo "  $INSTALL_DIR/dtscli"

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo
    echo "warning: $INSTALL_DIR is not in PATH"
    if [ "$UPDATE_PATH" = "1" ]; then
      SHELL_NAME=$(basename "${SHELL:-sh}")
      case "$SHELL_NAME" in
        zsh) PROFILE_FILE="$HOME/.zshrc" ;;
        bash) PROFILE_FILE="$HOME/.bashrc" ;;
        *) PROFILE_FILE="$HOME/.profile" ;;
      esac

      PATH_LINE="export PATH=\"$INSTALL_DIR:\$PATH\""
      if [ -f "$PROFILE_FILE" ] && grep -F "$PATH_LINE" "$PROFILE_FILE" >/dev/null 2>&1; then
        :
      else
        {
          echo
          echo "# Added by dtscli installer"
          echo "$PATH_LINE"
        } >> "$PROFILE_FILE"
      fi
      echo "added PATH update to $PROFILE_FILE"
      echo "reload your shell or run:"
      echo "  . \"$PROFILE_FILE\""
    else
      echo "add it to your shell profile, for example:"
      echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    fi
    ;;
esac

echo
echo "configure dt-main before starting DTS tasks:"
echo "  dtscli config set --workspace /path/to/ape-dts-release"
echo
echo "shell completions are not installed automatically; run this when needed:"
echo "  dtscli completion --help"
