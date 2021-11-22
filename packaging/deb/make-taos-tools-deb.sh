#!/bin/bash
#
# Generate deb package for ubuntu
set -e
# set -x

#curr_dir=$(pwd)
top_dir=$1
compile_dir=$2
output_dir=$3
taos_tools_ver=$4
cpuType=$5
osType=$6
verMode=$7
verType=$8

script_dir="$(dirname $(readlink -f $0))"
pkg_dir="${top_dir}/debworkroom"

#echo "curr_dir: ${curr_dir}"
#echo "top_dir: ${top_dir}"
#echo "script_dir: ${script_dir}"
echo "compile_dir: ${compile_dir}"
echo "pkg_dir: ${pkg_dir}"

if [ -d ${pkg_dir} ]; then
    rm -rf ${pkg_dir}
fi
mkdir -p ${pkg_dir}
cd ${pkg_dir}

# create install dir
install_home_path="/usr/local/taos"
mkdir -p ${pkg_dir}${install_home_path}
mkdir -p ${pkg_dir}${install_home_path}/bin || :

cp ${compile_dir}/build/bin/taosdump                ${pkg_dir}${install_home_path}/bin

install_user_local_path="/usr/local"

if [ -f ${compile_dir}/build/lib/libavro.so.23.0.0 ]; then
    mkdir -p ${pkg_dir}${install_user_local_path}/lib
    cp ${compile_dir}/build/lib/libavro.so.23.0.0 ${pkg_dir}${install_user_local_path}/lib/
    ln -sf libavro.so.23.0.0 ${pkg_dir}${install_user_local_path}/lib/libavro.so.23
    ln -sf libavro.so.23 ${pkg_dir}${install_user_local_path}/lib/libavro.so
fi
if [ -f ${compile_dir}/build/lib/libavro.a ]; then
    cp ${compile_dir}/build/lib/libavro.a ${pkg_dir}${install_user_local_path}/lib/
fi

cp -r ${top_dir}/src/kit/taos-tools/packaging/deb/DEBIAN        ${pkg_dir}/
chmod 755 ${pkg_dir}/DEBIAN/*

debname="taos-tools-"${taos_tools_ver}-${osType}-${cpuType}

if [ "$verType" == "beta" ]; then
  debname="taos-tools-"${taos_tools_ver}-${verType}-${osType}-${cpuType}".deb"
elif [ "$verType" == "stable" ]; then
  debname=${debname}".deb"
else
  echo "unknow verType, nor stabel or beta"
  exit 1
fi

# modify version of control
debver="Version: "$taos_tools_ver
sed -i "2c$debver" ${pkg_dir}/DEBIAN/control

# make deb package
dpkg -b ${pkg_dir} $debname
echo "make taos-tools deb package success!"

cp ${pkg_dir}/*.deb ${output_dir}

# clean temp dir
rm -rf ${pkg_dir}
