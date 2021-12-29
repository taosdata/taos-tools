
INCLUDE(ExternalProject)
ExternalProject_Add(
    apache-avro
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/avro
    SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
    CONFIGURE_COMMAND cmake -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}/build/ ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
)

ADD_SUBDIRECTORY(cJsonDemo)
ADD_SUBDIRECTORY(CUnit)