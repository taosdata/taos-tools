# taosTools
<div class="hide">
 
[![CI](https://github.com/taosdata/taos-tools/actions/workflows/cmake.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/cmake.yml)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/taos-tools/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/taos-tools?branch=develop)
 
 </div>

taosTools are some useful tool collections for TDengine.
 It includes `taosBenchmark` and `taosdump` right now.

taosBenchmark (once named taosdemo) can be used to stress test TDengine
 for full-featured writes, queries, subscriptions, etc. Please refer to
 the [taosBenchmark User Manual](https://github.com/taosdata/taos-tools/blob/develop/taosbenchmark-user-manual.md)
 for details on how to use it.

taosdump is a tool for backing up and restoring TDengine data to/from local directory.
 Please refer to the [taosdump User Manual](https://github.com/taosdata/taos-tools/blob/develop/taosdump-user-manual.md)
 for details on how to use it.

## Install taosTools

<ul id="taos-tools" class="package-list"></ul>

## How to build from source

### install dependencies

#### For Ubuntu/Debian system

```
sudo apt install libjansson-dev libsnappy-dev liblzma-dev libz-dev pkg-config
```

#### For CentOS

```
sudo yum install zlib-devel xz-devel snappy-devel jansson-devel pkgconfig libatomic libstdc++-static
```

Note: Since snappy lacks pkg-config support (refer to [link](https://github.com/google/snappy/pull/86)),
 it lead a cmake prompt libsnappy not found. But snappy will works well.

### install TDengine client

Please download TDengine client package from [taosdata.com](https://www.taosdata.com/cn/all-downloads/)
 or compile TDengine source from [github](github.com/taosdata/TDengine)
 and install to your system.

### clone source code and compile

```
git clone https://github.com/taosdata/taos-tools
cd taos-tools
git submodule update --init
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
