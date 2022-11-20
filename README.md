# taosTools

<div align="center">
<p>

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/7fb6f1cb61ab453580b69e48050dc9be)](https://app.codacy.com/gh/taosdata/taos-tools?utm_source=github.com&utm_medium=referral&utm_content=taosdata/taos-tools&utm_campaign=Badge_Grade_Settings)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/fe218d3468cb4c70af8320458d6c72a6)](https://www.codacy.com/gh/taosdata/taos-tools/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=taosdata/taos-tools&amp;utm_campaign=Badge_Grade)
[![CppCheck action](https://github.com/taosdata/taos-tools/actions/workflows/cppcheck.yml/badge.svg?branch=develop)](https://github.com/taosdata/taos-tools/actions/workflows/cppcheck.yml) [![CodeQL](https://github.com/taosdata/taos-tools/actions/workflows/codeql.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/codeql.yml) [![Coverage Status](https://coveralls.io/repos/github/taosdata/taos-tools/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/taos-tools?branch=develop)
<br />
[![Ubuntu (3.0 taosbenchmark release)](https://github.com/taosdata/taos-tools/actions/workflows/3.0-taosBenchmark-release.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/3.0-taosBenchmark-release.yml) [![Ubuntu (3.0 taosdump native)](https://github.com/taosdata/taos-tools/actions/workflows/3.0-taosdump-release.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/3.0-taosdump-release.yml) [![Windows (3.0 build)](https://github.com/taosdata/taos-tools/actions/workflows/windows-build-for3.0.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/windows-build-for3.0.yml)
<br />
[![taosBenchmark](https://github.com/taosdata/taos-tools/actions/workflows/ci-taosBenchmark-release.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/ci-taosBenchmark-release.yml) [![taosdump Release](https://github.com/taosdata/taos-tools/actions/workflows/ci-taosdump-release.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/ci-taosdump-release.yml) [![Windows (2.x build)](https://github.com/taosdata/taos-tools/actions/workflows/windows-build-for2.0.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/windows-build-for2.0.yml)

</p>
</div>

taosTools are some useful tool collections for TDengine.

taosBenchmark (once named taosdemo) can be used to stress test TDengine
for full-featured writes, queries, subscriptions, etc. In 2.4.0.7 and early release, taosBenchmark is distributed within taosTools package. In later release, taosBenchmark will be included within TDengine again. Please refer to
the [taosBenchmark User Manual](https://docs.tdengine.com/reference/taosbenchmark)
for details on how to use it.

taosdump is a tool for backing up and restoring TDengine data to/from local directory.
Please refer to the [taosdump User Manual](https://docs.tdengine.com/reference/taosdump)
for details on how to use it.

## Install taosTools

## How to build from source

### Install dependencies

#### For Ubuntu/Debian system

```shell
sudo apt install libjansson-dev libsnappy-dev liblzma-dev libz-dev pkg-config libssl-dev gawk
```

#### For CentOS 7/RHEL

```shell
sudo yum install -y zlib-devel xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libstdc++-static openssl-devel gawk
```

#### For CentOS 8/Rocky Linux

```shell
sudo yum install -y epel-release
sudo yum install -y dnf-plugins-core
sudo yum config-manager --set-enabled powertools
sudo yum install -y zlib-devel xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libstdc++-static openssl-devel gawk
```

Note: Since snappy lacks pkg-config support (refer to [link](https://github.com/google/snappy/pull/86)),
it lead a cmake prompt libsnappy not found. But snappy will works well.

#### For macOS (only taosBenchmark for now)

```shell
brew install argp-standalone gawk
```

### Install TDengine client

Please download TDengine client package from [tdengine.com](https://www.tdengine.com/cn/all-downloads/)
or compile TDengine source from [GitHub](github.com/taosdata/TDengine)
and install to your system.

### Clone source code and build

```shell
git clone https://github.com/taosdata/taos-tools
cd taos-tools
mkdir build
cd build
cmake ..
make
```

#### build taos-tools for TDengine 2.x

```shell
...
cmake .. -DTD_VER_COMPATIBLE=2.0.0.0
make
```

### Install

```shell
sudo make install
```
