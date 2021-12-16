
INCLUDE(ExternalProject)
ExternalProject_Add(
    apache-avro
    PREFIX              "avro"
    SOURCE_DIR          ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
    BUILD_IN_SOURCE     1
    CONFIGURE_COMMAND   cmake -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}/build/
)

ADD_SUBDIRECTORY(cJsonDemo)
ADD_SUBDIRECTORY(CUnit)