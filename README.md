# taos-tools
taos-tools are some useful tool collections for TDengine.

# How to build from source?

## install dependencies

### For Ubuntu/Debian system
sudo apt install libsnappy-dev liblzma-dev libz-dev

### For CentOS
sudo yum install xz-devel snappy-devel jansson-devel pkgconfig

## clone source code
git clone https://github.com/taosdata/taos-tools
cd taos-tools
git submodule update --init

## compile
mkdir build
cd build
cmake ..
make

## install
sudo make install

