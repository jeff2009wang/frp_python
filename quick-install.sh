#!/bin/bash

set -e

REPO="jeff2009wang/frp_python"
INSTALL_URL="https://raw.githubusercontent.com/${REPO}/main/install.sh"
TMP_SCRIPT="/tmp/pfrp_install.sh"

echo "Downloading install script..."
curl -fsSL "${INSTALL_URL}" -o "${TMP_SCRIPT}"
chmod +x "${TMP_SCRIPT}"

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root. Re-running with sudo..."
   sudo bash "${TMP_SCRIPT}" "$@"
else
   bash "${TMP_SCRIPT}" "$@"
fi
