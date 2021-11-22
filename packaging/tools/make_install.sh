#!/bin/bash

source_dir=$1

csudo=""

if command -v sudo > /dev/null; then
    csudo="sudo"
fi

[ ! -d /usr/local/taos/bin ] && mkdir -p /usr/local/taos/bin
${csudo} cp ${source_dir}/build/bin/taosdump /usr/local/taos/bin || echo -e "failed to copy taosdump"
${csudo} ln -sf /usr/local/taos/bin/taosdump /usr/local/bin/taosdump || echo -e "failed to link taosdump"
${csudo} cp -rf ${source_dir}/build/lib/* /usr/local/lib || echo -e "failed to copy libraries"
${csudo} ldconfig || echo -e "failed to run ldconfig"
