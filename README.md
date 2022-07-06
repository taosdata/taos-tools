# taosTools

<div class="hide">

[![CI](https://github.com/taosdata/taos-tools/actions/workflows/cmake.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/cmake.yml)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/taos-tools/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/taos-tools?branch=develop)

</div>

taosTools are some useful tool collections for TDengine.

taosBenchmark (once named taosdemo) can be used to stress test TDengine
 for full-featured writes, queries, subscriptions, etc. In 2.4.0.7 and early release, taosBenchmark is distributed within taosTools package. In later release, taosBenchmark will be included within TDengine again.  Please refer to
 the [taosBenchmark User Manual](https://docs.tdengine.com/reference/taosbenchmark)
 for details on how to use it.

taosdump is a tool for backing up and restoring TDengine data to/from local directory.
 Please refer to the [taosdump User Manual](https://docs.tdengine.com/reference/taosdump)
 for details on how to use it.

## Install taosTools

<ul id="taos-tools" class="package-list"></ul>

## How to build from source

### install dependencies

#### For Ubuntu/Debian system

```
sudo apt install libjansson-dev libsnappy-dev liblzma-dev libz-dev pkg-config
```

#### For CentOS 7/RHEL

```
sudo yum install -y zlib-devel xz-devel snappy-devel jansson jansson-devel pkgconfig libatomic libstdc++-static
```

#### For CentOS 8/Rocky Linux

```
sudo yum install -y epel-release
sudo yum install -y dnf-plugins-core
sudo yum config-manager --set-enabled powertools
sudo yum install -y zlib-devel xz-devel csnappy-devel jansson jansson-devel pkgconfig libatomic libstdc++-static
```

Note: Since snappy lacks pkg-config support (refer to [link](https://github.com/google/snappy/pull/86)),
 it lead a cmake prompt libsnappy not found. But snappy will works well.

#### For macOS (only taosBenchmark for now)
```
brew install argp-standalone
```

### install TDengine client

Please download TDengine client package from [taosdata.com](https://www.taosdata.com/cn/all-downloads/)
 or compile TDengine source from [github](github.com/taosdata/TDengine)
 and install to your system.

### clone source code and compile

```
git clone https://github.com/taosdata/taos-tools
cd taos-tools
git submodule update --init --recursive
mkdir build
cd build
cmake ..
make
```

### install

```
sudo make install
```

<script src="/wp-includes/js/quick-start.js?v=1"></script>
