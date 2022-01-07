#!/bin/bash
#
# This file is used to install taos-tools on your linux systems.
#

set -e
#set -x

# -----------------------Variables definition---------------------
script_dir=$(dirname $(readlink -f "$0"))
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
    csudo="sudo"
fi

# get the operating system type for using the corresponding init file
# ubuntu/debian(deb), centos/fedora(rpm), others: opensuse, redhat, ..., no verification
#osinfo=$(awk -F= '/^NAME/{print $2}' /etc/os-release)
if [[ -e /etc/os-release ]]; then
    osinfo=$(cat /etc/os-release | grep "NAME" | cut -d '"' -f2)   ||:
else
    osinfo=""
fi
#echo "osinfo: ${osinfo}"
os_type=0
if echo $osinfo | grep -qwi "ubuntu" ; then
    #  echo "This is ubuntu system"
    os_type=1
elif echo $osinfo | grep -qwi "debian" ; then
    #  echo "This is debian system"
    os_type=1
elif echo $osinfo | grep -qwi "Kylin" ; then
    #  echo "This is Kylin system"
    os_type=1
elif  echo $osinfo | grep -qwi "centos" ; then
    #  echo "This is centos system"
    os_type=2
elif echo $osinfo | grep -qwi "fedora" ; then
    #  echo "This is fedora system"
    os_type=2
elif echo $osinfo | grep -qwi "Linx" ; then
    #  echo "This is Linx system"
    os_type=1
else
    echo " osinfo: ${osinfo}"
    echo " This is an officially unverified linux system,"
    echo " if there are any problems with the installation and operation, "
    echo " please feel free to contact taosdata.com for support."
    os_type=1
fi

function kill_process() {
  pid=$(ps -ef | grep "$1" | grep -v "grep" | awk '{print $2}')
  if [ -n "$pid" ]; then
    ${csudo} kill -9 $pid   || :
  fi
}

function install_main_path() {
    #create install main dir and all sub dir
    [[ ! -d ${install_main_dir}/bin ]] && ${csudo} mkdir -p ${install_main_dir}/bin || :
}

function install_bin() {
    # Remove links
    ${csudo} rm -f ${bin_link_dir}/taosdemo         || :
    ${csudo} rm -f ${bin_link_dir}/taosBenchmark    || :
    ${csudo} rm -f ${bin_link_dir}/taosdump         || :

    ${csudo} /usr/bin/install -c -m 755 ${script_dir}/bin/taosdump ${install_main_dir}/bin/taosdump
    ${csudo} /usr/bin/install -c -m 755 ${script_dir}/bin/taosBenchmark ${install_main_dir}/bin/taosBenchmark
    ${csudo} ln -sf ${install_main_dir}/bin/taosBenchmark ${install_main_dir}/bin/taosdemo
    #Make link
    [[ -x ${install_main_dir}/bin/taosBenchmark ]] && \
        ${csudo} ln -s ${install_main_dir}/bin/taosBenchmark ${bin_link_dir}/taosBenchmark  || :
    [[ -x ${install_main_dir}/bin/taosdemo ]] && \
        ${csudo} ln -s ${install_main_dir}/bin/taosdemo ${bin_link_dir}/taosdemo            || :
    [[ -x ${install_main_dir}/bin/taosdump ]] && \
        ${csudo} ln -s ${install_main_dir}/bin/taosdump ${bin_link_dir}/taosdump            || :
}

function install_avro() {
    if [ "$osType" != "Darwin" ]; then
        avro_dir=${script_dir}/avro
        if [ -f "${avro_dir}/lib/libavro.so.23.0.0" ] && [ -d /usr/local/$1 ]; then
            ${csudo} /usr/bin/install -c -d /usr/local/$1
            ${csudo} /usr/bin/install -c -m 755 ${avro_dir}/lib/libavro.so.23.0.0 /usr/local/$1
            ${csudo} ln -sf /usr/local/$1/libavro.so.23.0.0 /usr/local/$1/libavro.so.23
            ${csudo} ln -sf /usr/local/$1/libavro.so.23 /usr/local/$1/libavro.so

            ${csudo} /usr/bin/install -c -d /usr/local/$1
            [ -f ${avro_dir}/lib/libavro.a ] &&
                ${csudo} /usr/bin/install -c -m 755 ${avro_dir}/lib/libavro.a /usr/local/$1

            if [ -d /etc/ld.so.conf.d ]; then
                echo "/usr/local/$1" | ${csudo} tee /etc/ld.so.conf.d/libavro.conf > /dev/null || echo -e "failed to write /etc/ld.so.conf.d/libavro.conf"
                ${csudo} ldconfig
            else
                echo "/etc/ld.so.conf.d not found!"
            fi
        fi
    fi
}


function install_taostools() {
    # Start to install
    echo -e "${GREEN}Start to install taos tools ...${NC}"

    install_main_path

    install_avro lib
    install_avro lib64


    # For installing new
    install_bin

    echo
    echo -e "\033[44;32;1mtaos tools is installed successfully!${NC}"
}

function uninstall_bin() {
    # Remove links
    ${csudo} rm -f ${bin_link_dir}/taosdemo         || :
    ${csudo} rm -f ${bin_link_dir}/taosBenchmark    || :
    ${csudo} rm -f ${bin_link_dir}/taosdump         || :

    ${csudo} rm -f ${install_main_dir}/bin/taosdemo         || :
    ${csudo} rm -f ${install_main_dir}/bin/taosBenchmark    || :
    ${csudo} rm -f ${install_main_dir}/bin/taosdump         || :
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
if [ "$1" == "uninstall" ]; then
    uninstall_taostools
else
    install_taostools
fi
