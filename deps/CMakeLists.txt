
INCLUDE(ExternalProject)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        MESSAGE("will not build jansson, use system's")
    ELSE ()
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

    SET(OS_ID "")
    EXECUTE_PROCESS (
        COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
        OUTPUT_VARIABLE OS_ID
        )
    MESSAGE(STATUS "deps' cmake detected current OS is " ${OS_ID} "")
    ExternalProject_Add(
        deps-jansson
        GIT_REPOSITORY https://github.com/akheron/jansson
        GIT_TAG         v2.14
        GIT_SHALLOW     true
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/jansson
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/jansson
        CONFIGURE_COMMAND cmake -DJANSSON_BUILD_DOCS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_INSTALL_LIBDIR=lib -S ${PROJECT_SOURCE_DIR}/deps/jansson
        )

    ExternalProject_Add(
        deps-snappy
        GIT_REPOSITORY https://github.com/google/snappy
        GIT_TAG         1.1.8
        GIT_SHALLOW     true
        GIT_SUBMODULES  ""
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/snappy
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/snappy
        CONFIGURE_COMMAND cmake -DBUILD_SHARED_LIBS=OFF -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_INSTALL_LIBDIR=lib -S ${PROJECT_SOURCE_DIR}/deps/snappy
        )

    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        ExternalProject_Add(
            apache-avro
            GIT_REPOSITORY https://github.com/apache/avro
            GIT_TAG         release-1.11.0
            GIT_SHALLOW     true
            PREFIX ${CMAKE_CURRENT_BINARY_DIR}/avro
            SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/avro
            PATCH_COMMAND cmake -E copy ${PROJECT_SOURCE_DIR}/deps/datafile.c ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/datafile.c
            CONFIGURE_COMMAND cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_INSTALL_LIBDIR=lib -S ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
            )
    ELSE ()
        ExternalProject_Add(
            apache-avro
            GIT_REPOSITORY https://github.com/apache/avro
            GIT_TAG         release-1.11.0
            GIT_SHALLOW     true
            PREFIX ${CMAKE_CURRENT_BINARY_DIR}/avro
            SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/avro
            PATCH_COMMAND cmake -E copy ${PROJECT_SOURCE_DIR}/deps/datafile.c ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/datafile.c
            CONFIGURE_COMMAND cmake -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -DCMAKE_INSTALL_LIBDIR=lib -S ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
            )
    ENDIF ()
    ADD_SUBDIRECTORY(CUnit)
ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
ELSEIF(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        MESSAGE("will not build jansson, use system's")
    ELSE ()
        # add_compile_options(-Wno-error=sign-compare)
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

    MESSAGE(STATUS "deps' cmake detected current OS is Windows")
    ExternalProject_Add(
        deps-jansson
        GIT_REPOSITORY https://github.com/akheron/jansson
        GIT_TAG         v2.14
        GIT_SHALLOW     true
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/jansson
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/jansson
        CONFIGURE_COMMAND cmake -G "NMake Makefiles JOM" -DCMAKE_MAKE_PROGRAM=jom -DJANSSON_BUILD_DOCS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/jansson
        )

    ExternalProject_Add(
        deps-libargp
        GIT_REPOSITORY  https://github.com/divisionind/libargp-mingw.git
        GIT_TAG         master
        GIT_SHALLOW     true
        GIT_SUBMODULES  ""
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/libargp/
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/libargp
        PATCH_COMMAND sed -i "s/strcasecmp/_stricmp/g" ${PROJECT_SOURCE_DIR}/deps/libargp/src/argp-help.c && sed -i "s/#include <unistd.h>/#define ssize_t int/g" ${PROJECT_SOURCE_DIR}/deps/libargp/src/argp-fmtstream.h && sed -i "s/#include <unistd.h>//g" ${PROJECT_SOURCE_DIR}/deps/libargp/src/argp-parse.c && sed -i "s/#ifdef __MINGW32__/#ifndef __MINGW32__/g" ${PROJECT_SOURCE_DIR}/deps/libargp/src/argp-parse.c && sed -i "s/#include <unistd.h>//g" ${PROJECT_SOURCE_DIR}/deps/libargp/src/getopt_init.c && sed -i "s/add_executable(libargp-mingw \${SRC_LIST} argp-test.c)/ADD_LIBRARY(libargp \${SRC_LIST})\\ninstall(TARGETS libargp ARCHIVE DESTINATION lib)\\ninstall(FILES \${CMAKE_CURRENT_SOURCE_DIR}\\/src\\/argp.h DESTINATION include)/g" ${PROJECT_SOURCE_DIR}/deps/libargp/CMakeLists.txt
        CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/"
        )

    ExternalProject_Add(
        deps-snappy
        GIT_REPOSITORY https://github.com/google/snappy
        GIT_TAG         1.1.8
        GIT_SHALLOW     true
        GIT_SUBMODULES  ""
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/snappy
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/snappy
        CONFIGURE_COMMAND cmake -G "NMake Makefiles JOM" -DCMAKE_MAKE_PROGRAM=jom -DBUILD_SHARED_LIBS=OFF -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/snappy
        )

    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        SET(BUILD_TYPE "-DCMAKE_BUILD_TYPE=Debug")
    ELSE ()
        SET(BUILD_TYPE "")
    ENDIF ()
    ExternalProject_Add(
        apache-avro
        GIT_REPOSITORY https://github.com/apache/avro
        GIT_TAG         release-1.11.0
        GIT_SHALLOW     true
        PREFIX ${CMAKE_CURRENT_BINARY_DIR}/avro
        SOURCE_DIR ${PROJECT_SOURCE_DIR}/deps/avro
        PATCH_COMMAND sed -i "s/^install(TARGETS avro[^-]/# install(TARGETS avro/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/CMakeLists.txt && sed -i "s/^target_link_libraries(avro[^-]/# target_link_libraries(avro/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/CMakeLists.txt && sed -i "s/^add_executable(avro/# add_executable(avro/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/CMakeLists.txt && sed -ri "s/([^_])namespace/\\1taos_namespace/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/schema.c && sed -i "s/strrchr(fullname/strrchr((char *)fullname/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/schema.c && sed -i "s/^#include <stdlib.h>/#define ssize_t int\\n #include <stdlib.h>/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/generic.c && sed -i "s/, codec->block_data,/, (uint8_t *)(codec->block_data),/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/codec.c && sed -i "s/filters, NULL, data/filters, NULL, (const uint8_t *)data/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/codec.c && sed -i "s/__cdecl//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msinttypes.h && sed -i "s/imaxdiv_t/imaxdiv1_t/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msinttypes.h && sed -i "s/imaxdiv(/imax_div(/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msinttypes.h && sed -i "s/typedef uint16_t  uint_fast16_t//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msstdint.h && sed -i "s/typedef int16_t   int_fast16_t//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro/msstdint.h && sed -i "s/#define snprintf _snprintf//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/avro_private.h && sed -i "s/^include_directories(\${AvroC_SOURCE_DIR}\\/src)/include_directories(\${AvroC_SOURCE_DIR}\\/src)\\ninclude_directories(\${CMAKE_INSTALL_PREFIX}\\/include)/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && sed -i "s/add_subdirectory(examples)//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && sed -i "s/add_subdirectory(tests)//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && sed -i "s/add_subdirectory(docs)//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && sed -i "s/add_definitions(-DDEFLATE_CODEC)//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && sed -i "s/add_definitions(-DSNAPPY_CODEC)//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && sed -i "s/ include_directories/ # include_directories/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && sed -i "s/add_definitions(-DLZMA_CODEC)//g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && sed -i "s/JANSSON_FOUND/TRUE/g" ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/CMakeLists.txt && cmake -E copy ${PROJECT_SOURCE_DIR}/deps/datafile.c ${PROJECT_SOURCE_DIR}/deps/avro/lang/c/src/datafile.c
        CONFIGURE_COMMAND cmake -G "NMake Makefiles JOM" ${BUILD_TYPE} -DCMAKE_MAKE_PROGRAM=jom -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/build/ -S ${PROJECT_SOURCE_DIR}/deps/avro/lang/c
        )
    ADD_SUBDIRECTORY(CUnit)
ENDIF()
ADD_SUBDIRECTORY(toolscJson)
