CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

PROJECT(taos-tool
    LANGUAGES C
    VERSION 0.1.0
)

if(NOT WIN32)
  string(ASCII 27 Esc)
  set(ColourReset "${Esc}[m")
  set(ColourBold  "${Esc}[1m")
  set(Red         "${Esc}[31m")
  set(Green       "${Esc}[32m")
  set(Yellow      "${Esc}[33m")
  set(Blue        "${Esc}[34m")
  set(Magenta     "${Esc}[35m")
  set(Cyan        "${Esc}[36m")
  set(White       "${Esc}[37m")
  set(BoldRed     "${Esc}[1;31m")
  set(BoldGreen   "${Esc}[1;32m")
  set(BoldYellow  "${Esc}[1;33m")
  set(BoldBlue    "${Esc}[1;34m")
  set(BoldMagenta "${Esc}[1;35m")
  set(BoldCyan    "${Esc}[1;36m")
  set(BoldWhite   "${Esc}[1;37m")
endif()

find_package(PkgConfig REQUIRED)

find_package(ZLIB)
if (ZLIB_FOUND)
    set(ZLIB_PKG zlib)
    add_definitions(-DDEFLATE_CODEC)
    include_directories(${ZLIB_INCLUDE_DIRS})
    message("${Green} Enabled deflate codec ${ColourReset}")
else (ZLIB_FOUND)
    set(ZLIB_PKG "")
    set(ZLIB_LIBRARIES "")
    message("${Red} Disabled deflate codec. zlib not found. ${ColourReset}")
endif (ZLIB_FOUND)

pkg_check_modules(SNAPPY snappy)
if (SNAPPY_FOUND AND ZLIB_FOUND)  # Snappy borrows crc32 from zlib
    set(SNAPPY_PKG libsnappy)
    add_definitions(-DSNAPPY_CODEC)
    include_directories(${SNAPPY_INCLUDE_DIRS})
    message("${Green} Enabled snappy codec ${ColourReset}")
else ()
    set(SNAPPY_PKG "")
    set(SNAPPY_LIBRARIES "")
    message("${Red} Disabled snappy codec. libsnappy not found or zlib not found.${ColourReset}")
endif (SNAPPY_FOUND AND ZLIB_FOUND)

find_package(PkgConfig)
pkg_check_modules(LZMA liblzma)
if (LZMA_FOUND)
    set(LZMA_PKG liblzma)
    add_definitions(-DLZMA_CODEC)
    include_directories(${LZMA_INCLUDE_DIRS})
    link_directories(${LZMA_LIBRARY_DIRS})
    message("${Green} Enabled lzma codec ${ColourReset}")
else (LZMA_FOUND)
    set(LZMA_PKG "")
    set(LZMA_LIBRARIES "")
    message("${Red} Disabled lzma codec. liblzma not found.${ColourReset}")
endif (LZMA_FOUND)

# Jansson JSON library
pkg_check_modules(JANSSON jansson>=2.3)
IF (JANSSON_FOUND)
    MESSAGE("${Green} jansson found ${ColourReset}")
ELSE()
    MESSAGE(FATAL_ERROR "${Red} jansson not found ${ColourReset}")
ENDIF()

SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/build/bin)

ADD_SUBDIRECTORY(deps)
ADD_SUBDIRECTORY(src)

ADD_DEPENDENCIES(taosdump apache-avro)
ADD_DEPENDENCIES(taosdemo cJsonDemo)

SET(MAKE_INSTALL_SH "${PROJECT_SOURCE_DIR}/packaging/tools/make_install.sh")
INSTALL(CODE "execute_process(COMMAND bash ${MAKE_INSTALL_SH} ${CMAKE_BINARY_DIR})")

