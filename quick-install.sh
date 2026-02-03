#!/bin/bash

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root"
   sudo bash -c "$(curl -fsSL https://raw.githubusercontent.com/jeff2009wang/frp_python/main/install.sh)"
   exit $?
else
   curl -fsSL https://raw.githubusercontent.com/jeff2009wang/frp_python/main/install.sh -o /tmp/pfrp_install.sh
   chmod +x /tmp/pfrp_install.sh
   /tmp/pfrp_install.sh "$@"
fi
