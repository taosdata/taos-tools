
INCLUDE(ExternalProject)

OPTION(JANSSON_BUILD_DOCS "turn jansson build docs off" OFF)
OPTION(JANSSON_INSTALL "turn jansson install off" OFF)

ExternalProject_Add(
    deps-jansson
    GIT_REPOSITORY https://github.com/akheron/jansson
    GIT_TAG         v2.14
    GIT_SHALLOW     true
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/jansson
    SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/jansson
    CONFIGURE_COMMAND cmake -DJANSSON_BUILD_DOCS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/jansson
    )

ExternalProject_Add(
    deps-snappy
    GIT_REPOSITORY https://github.com/google/snappy
    GIT_TAG         1.1.9
    GIT_SHALLOW     true
    GIT_SUBMODULES  ""
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/snappy
    SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/snappy
    CONFIGURE_COMMAND cmake -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/snappy
    )

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    IF (${TOOLS_BUILD_TYPE} MATCHES "debug")
        MESSAGE("will not build jansson and snappy, use system's")
    ELSE ()
        OPTION(BUILD_SHARED_LIBS "turn snappy build shared lib off" OFF)
        OPTION(SNAPPY_BUILD_TESTS "turn snappy build tests off" OFF)
        OPTION(SNAPPY_BUILD_BENCHMARKS "turn snappy build benchmarks off" OFF)
        OPTION(SNAPPY_INSTALL "turn snappy install off" OFF)
        add_compile_options(-Wno-error=sign-compare)
        if(TD_ARM_32)
            MESSAGE("-- temporary change flag for snappy")
            IF (${CMAKE_BUILD_TYPE} MATCHES "Debug")
                set (TEMP_CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
                string(REPLACE "-Wno-pointer-to-int-cast" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
                string(REPLACE "-Wno-incompatible-pointer-types" "" CMAKE_CXX_FLAGS_DEBUG ${CMAKE_CXX_FLAGS_DEBUG})
                set(CMAKE_CXX_FLAGS_DEBUG ${TEMP_CMAKE_CXX_FLAGS_DEBUG})
            ELSE ()
                set (TEMP_CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
                string(REPLACE "-Wno-pointer-to-int-cast" "" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
                string(REPLACE "-Wno-incompatible-pointer-types" "" CMAKE_CXX_FLAGS_RELEASE ${CMAKE_CXX_FLAGS_RELEASE})
                set(CMAKE_CXX_FLAGS_RELEASE ${TEMP_CMAKE_CXX_FLAGS_RELEASE})
            ENDIF ()
        ENDIF()
    ENDIF ()

    ExternalProject_Add(
        apache-avro
        GIT_REPOSITORY https://github.com/apache/avro
        GIT_TAG         release-1.11.0
        GIT_SHALLOW     true
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/avro
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/avro
        CONFIGURE_COMMAND cmake -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
    )
    ADD_SUBDIRECTORY(CUnit)
    ADD_DEPENDENCIES(apache-avro deps-jansson)
    ADD_DEPENDENCIES(apache-avro deps-snappy)
ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
ENDIF()
ADD_SUBDIRECTORY(toolscJson)
