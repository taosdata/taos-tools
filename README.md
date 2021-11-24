# taos-tools
taos-tools are some useful tool collections for TDengine.

# How to build from source?

## install dependencies

### For Ubuntu/Debian system
```
sudo apt install libjansson-dev libsnappy-dev liblzma-dev libz-dev pkg-config
```

### For CentOS
```
sudo yum install xz-devel snappy-devel jansson-devel pkgconfig libatomic
```
Note: Since snappy lacks pkg-config support (refer to [link](https://github.com/google/snappy/pull/86)), it lead a cmake prompt libsnappy not found. But snappy will works well.

## install TDengine client
Please download TDengine client package from [taosdata.com](https://www.taosdata.com/cn/all-downloads/) or compile TDengine source from [github](github.com/taosdata/TDengine) and install to your system.

## clone source code and compile
```
git clone https://github.com/taosdata/taos-tools
cd taos-tools
git submodule update --init
mkdir build
cd build
cmake ..
make
```

## install
```
sudo make install
```

