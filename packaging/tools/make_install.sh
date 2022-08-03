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
${csudo}cp ${source_dir}/build/bin/${dumpName} ${installDir}/bin || echo -e "failed to copy ${dumpName}"
${csudo}cp ${source_dir}/build/bin/${benchmarkName} ${installDir}/bin || echo -e "failed to copy ${benchmarkName}"
${csudo}ln -sf ${installDir}/bin/${dumpName} /usr/local/bin/${dumpName} || echo -e "failed to link ${dumpName}"
${csudo}ln -sf ${installDir}/bin/${benchmarkName} /usr/local/bin/${benchmarkName} || echo -e "failed to link ${benchmarkName}"
${csudo}ln -sf ${installDir}/bin/${benchmarkName} /usr/local/bin/${demoName} || echo -e "failed to link ${benchmarkName} as ${demoName}"

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
