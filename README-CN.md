# taosTools

<div class="hide">
 [![CI](https://github.com/taosdata/taos-tools/actions/workflows/cmake.yml/badge.svg)](https://github.com/taosdata/taos-tools/actions/workflows/cmake.yml)
[![Coverage Status](https://coveralls.io/repos/github/taosdata/taos-tools/badge.svg?branch=develop)](https://coveralls.io/github/taosdata/taos-tools?branch=develop)
 </div>

taosTools 是用于 TDengine 的辅助工具软件集合。目前它包含 taosBenchmark 和 taosdump 两个工具软件。

taosBenchmark （曾命名为 taosdemo）可以用于对 TDengine 进行全功能的写入、查询、订阅等功能的压力测试。详细使用方法请参考[taosBenchmark用户手册](https://github.com/taosdata/taos-tools/blob/develop/taosbenchmark-user-manual-CN.md)。

taosdump 是用于备份 TDengine 数据到本地目录和从本地目录恢复数据到 TDengine 的工具。详细使用方法请参考[taosdump用户手册](https://github.com/taosdata/taos-tools/blob/develop/taosdump-user-manual-CN.md)。

## 安装 taosTools

<ul id="taos-tools" class="package-list"></ul>

## 如何通过源代码构建？

### 安装依赖软件包

#### 对于 Ubuntu/Debian 系统

```
sudo apt install libjansson-dev libsnappy-dev liblzma-dev libz-dev pkg-config
```

#### 对于 CentOS/RHEL 系统

```
sudo yum install xz-devel snappy-devel jansson-devel pkgconfig libatomic libstdc++-static
```

注意：由于 snappy 缺乏 pkg-config 支持
（参考 [链接](https://github.com/google/snappy/pull/86)），会导致
 cmake 提示无法发现 libsnappy，实际上工作正常。

### 安装 TDengine 客户端软件

请从 [taosdata.com](https://www.taosdata.com/cn/all-downloads/) 下载
 TDengine 客户端安装或参考 [github](github.com/taosdata/TDengine)
 编译安装 TDengine 到您的系统中。

### 克隆源码并编译

```
git clone https://github.com/taosdata/taos-tools
cd taos-tools
git submodule update --init
mkdir build
cd build
cmake ..
make
```

### 安装

```
sudo make install
```

<script src="/wp-includes/js/quick-start.js?v=1"></script>
