#!/bin/bash

#set -x

installDir="/usr/local/taos"
dumpName="taosdump"
benchmarkName="taosBenchmark"
demoName="taosdemo"

source_dir=$1

csudo=""

if command -v sudo > /dev/null; then
    csudo="sudo "
fi

[ ! -d ${installDir}/bin ] && mkdir -p ${installDir}/bin
[ -f ${source_dir}/build/bin/${dumpName} ] && ${csudo}cp ${source_dir}/build/bin/${dumpName} ${installDir}/bin ||:
[ -f ${source_dir}/build/bin/${benchmarkName} ] && ${csudo}cp ${source_dir}/build/bin/${benchmarkName} ${installDir}/bin ||:
[ -f ${installDir}/bin/${dumpName} ] && ${csudo}ln -sf ${installDir}/bin/${dumpName} /usr/local/bin/${dumpName} ||:
[ -f ${installDir}/bin/${benchmarkName} ] && ${csudo}ln -sf ${installDir}/bin/${benchmarkName} /usr/local/bin/${benchmarkName} ||:
[ -f ${installDir}/bin/${benchmarkName} ] && ${csudo}ln -sf ${installDir}/bin/${benchmarkName} /usr/local/bin/${demoName} ||:

#if [ -f ${source_dir}/build/lib/libavro.so.23.0.0 ]; then
#    ${csudo}cp -rf ${source_dir}/build/lib/libavro* /usr/local/lib > /dev/null || echo -e "failed to copy avro libraries"
#    ${csudo}cp -rf ${source_dir}/build/lib/pkgconfig /usr/local/lib > /dev/null || echo -e "failed to copy pkgconfig directory"
#fi

#if [ -f ${source_dir}/build/lib64/libavro.so.23.0.0 ]; then
#    ${csudo}cp -rf ${source_dir}/build/lib64/libavro* /usr/local/lib > /dev/null || echo -e "failed to copy avro libraries"
#    ${csudo}cp -rf ${source_dir}/build/lib64/pkgconfig /usr/local/lib > /dev/null || echo -e "failed to copy pkgconfig directory"
#fi

#if [ -d /etc/ld.so.conf.d ]; then
#    echo "/usr/local/lib" | ${csudo}tee /etc/ld.so.conf.d/libavro.conf > /dev/null || echo -e "failed to write /etc/ld.so.conf.d/libavro.conf"
#    ${csudo}ldconfig || echo -e "failed to run ldconfig"
#else
#    echo "/etc/ld.so.conf.d not found!"
#fi
