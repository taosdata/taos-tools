#!/bin/bash
#
# This file is used to install taos-tools on your linux systems.
#

set -e
#set -x

demoName="taosdemo"
benchmarkName="taosBenchmark"
dumpName="taosdump"
taosName="taos"
toolsName="taostools"

# -----------------------Variables definition---------------------
# Dynamic directory
bin_link_dir="/usr/bin"

#install main path
install_main_dir="/usr/local/taos"

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

function uninstall_libtaosws() {
    if [ ! -f ${install_main_dir}/bin/taos ]; then
        [ -f /usr/lib/libtaosws.so ] && ${csudo}rm -f /usr/lib/libtaosws.so ||:
        [ -f /usr/lib64/libtaosws.so ] && ${csudo}rm -f /usr/lib64/libtaosws.so ||:
        [ -f ${install_main_dir}/driver/libtaosws.so ] && ${csudo}rm -f ${install_main_dir}/driver/libtaosws.so ||:
    fi
}

function uninstall_bin() {
    # Remove links
    ${csudo}rm -f ${bin_link_dir}/${demoName}         || :
    ${csudo}rm -f ${bin_link_dir}/${benchmarkName}    || :
    ${csudo}rm -f ${bin_link_dir}/${dumpName}         || :
    ${csudo}rm -f ${bin_link_dir}/rm${toolsName}      || :

    ${csudo}rm -f ${install_main_dir}/bin/${demoName}                  || :
    ${csudo}rm -f ${install_main_dir}/bin/${benchmarkName}             || :
    ${csudo}rm -f ${install_main_dir}/bin/${dumpName}                  || :
    ${csudo}rm -f ${install_main_dir}/bin/uninstall-${toolsName}.sh    || :
}


function uninstall_taostools() {
    # Start to uninstall
    echo -e "${GREEN}Start to uninstall ${taosName} tools ...${NC}"

    kill_process ${demoName}
    kill_process ${benchmarkName}
    kill_process ${dumpName}

    uninstall_bin
    uninstall_libtaosws

    echo
    echo -e "\033[44;32;1m${taosName} tools is uninstalled successfully!${NC}"
}

## ==============================Main program starts from here============================
uninstall_taostools
