# taos-tools
[![codecov](https://codecov.io/gh/taosdata/taos-tools/branch/develop/graph/badge.svg?token=W7Z6XKIKV9)](https://codecov.io/gh/taosdata/taos-tools)
taos-tools 是多个用于 TDengine 的辅助工具软件集合。

# 如何通过源代码构建？

## 安装依赖软件包

### 对于 Ubuntu/Debian 系统
```
sudo apt install libjansson-dev libsnappy-dev liblzma-dev libz-dev pkg-config
```

### 对于 CentOS/RHEL 系统
```
sudo yum install xz-devel snappy-devel jansson-devel pkgconfig libatomic
```
注意：由于 snappy 缺乏 pkg-config 支持 （参考 [链接](https://github.com/google/snappy/pull/86)），会导致 cmake 提示无法发现 libsnappy，实际上工作正常。 

## 安装 TDengine 客户端软件
请从 [taosdata.com](https://www.taosdata.com/cn/all-downloads/) 下载 TDengine 客户端安装或参考 [github](github.com/taosdata/TDengine) 编译安装 TDengine 到您的系统中。

## 克隆源码并编译
```
git clone https://github.com/taosdata/taos-tools
cd taos-tools
git submodule update --init
mkdir build
cd build
cmake ..
make
```

## 安装
```
sudo make install
```

