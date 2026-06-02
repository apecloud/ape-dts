#!/usr/bin/env sh
set -eu

INSTALL_DIR=${INSTALL_DIR:-"$HOME/.local/bin"}
REMOVE_DATA=${REMOVE_DATA:-0}

rm -f "$INSTALL_DIR/dtscli" "$INSTALL_DIR/dt-main" "$INSTALL_DIR/log4rs.yaml"

echo "removed binaries from $INSTALL_DIR"

if [ "$REMOVE_DATA" = "1" ]; then
  rm -rf "$HOME/.ape-dts"
  echo "removed $HOME/.ape-dts"
else
  echo "kept $HOME/.ape-dts task records and config"
  echo "set REMOVE_DATA=1 to remove local dtscli data during uninstall"
fi
