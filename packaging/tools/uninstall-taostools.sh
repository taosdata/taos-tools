#!/bin/bash
#
# This file is used to install taos-tools on your linux systems.
#

set -e
#set -x

# -----------------------Variables definition---------------------
# Dynamic directory
bin_link_dir="/usr/bin"

#install main path
install_main_dir="/usr/local"

# Color setting
RED='\033[0;31m'
GREEN='\033[1;32m'
GREEN_DARK='\033[0;32m'
GREEN_UNDERLINE='\033[4;32m'
NC='\033[0m'

csudo=""
if command -v sudo > /dev/null; then
    csudo="sudo "
fi

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo}kill -9 $pid   || :
  fi
}

function uninstall_bin() {
    # Remove links
    ${csudo}rm -f ${bin_link_dir}/taosdemo         || :
    ${csudo}rm -f ${bin_link_dir}/taosBenchmark    || :
    ${csudo}rm -f ${bin_link_dir}/taosdump         || :
    ${csudo}rm -f ${bin_link_dir}/rmtaostools      || :

    ${csudo}rm -f ${install_main_dir}/bin/taosdemo                  || :
    ${csudo}rm -f ${install_main_dir}/bin/taosBenchmark             || :
    ${csudo}rm -f ${install_main_dir}/bin/taosdump                  || :
    ${csudo}rm -f ${install_main_dir}/bin/uninstall-taostools.sh    || :
}


function uninstall_taostools() {
    # Start to uninstall
    echo -e "${GREEN}Start to uninstall taos tools ...${NC}"

    kill_process taosdemo
    kill_process taosBenchmark
    kill_process taosdump

    uninstall_bin

    echo
    echo -e "\033[44;32;1mtaos tools is uninstalled successfully!${NC}"
}

## ==============================Main program starts from here============================
uninstall_taostools
