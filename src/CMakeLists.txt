CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
INCLUDE_DIRECTORIES(../inc)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/toolscJson/inc)

MESSAGE(STATUS "The current platform " ${CMAKE_SYSTEM_PROCESSOR} " is detected")

IF (CMAKE_SYSTEM_PROCESSOR MATCHES "(amd64)|(AMD64)|(x86_64)")
    MESSAGE(STATUS "The current platform is amd64")
    MESSAGE(STATUS "Set CPUTYPE to x64")
    SET(CURRENT_CPUTYPE "x64")
    SET(CURRENT_PLATFORM_ARCH_STR "amd64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)")
    MESSAGE(STATUS "The current platform is x86")
    MESSAGE(STATUS "Set CPUTYPE to x86")
    SET(CURRENT_CPUTYPE "x32")
    SET(CURRENT_PLATFORM_ARCH_STR "i386")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "armv7l")
    MESSAGE(STATUS "Set CPUTYPE to aarch32")
    SET(CURRENT_CPUTYPE "aarch32")
    MESSAGE(STATUS "Set CPUTYPE to aarch32")
    SET(CURRENT_PLATFORM_ARCH_STR "arm")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    SET(CURRENT_CPUTYPE "aarch64")
    MESSAGE(STATUS "Set CPUTYPE to aarch64")
    SET(CURRENT_PLATFORM_ARCH_STR "arm64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "mips64")
    SET(CURRENT_CPUTYPE "mips64")
    MESSAGE(STATUS "Set CPUTYPE to mips64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
    SET(CURRENT_CPUTYPE "apple_m1")
    MESSAGE(STATUS "Set CPUTYPE to apple silicon m1")
ENDIF ()

FIND_PACKAGE(Git)
IF(GIT_FOUND)
    EXECUTE_PROCESS(
            COMMAND ${GIT_EXECUTABLE} log --pretty=oneline -n 1 ${CMAKE_CURRENT_LIST_DIR}/taosdump.c
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSDUMP_COMMIT_SHA1
    )
    EXECUTE_PROCESS(
            COMMAND ${GIT_EXECUTABLE} log --pretty=oneline -n 1 ${CMAKE_CURRENT_LIST_DIR}
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE taosbenchmark_COMMIT_SHA1
    )
    IF ("${TAOSDUMP_COMMIT_SHA1}" STREQUAL "")
        SET(TAOSDUMP_COMMIT_SHA1 "unknown")
    ELSE ()
        STRING(SUBSTRING "${TAOSDUMP_COMMIT_SHA1}" 0 7 TAOSDUMP_COMMIT_SHA1)
        STRING(STRIP "${TAOSDUMP_COMMIT_SHA1}" TAOSDUMP_COMMIT_SHA1)
    ENDIF ()
    IF ("${taosbenchmark_COMMIT_SHA1}" STREQUAL "")
        SET(taosbenchmark_COMMIT_SHA1 "unknown")
    ELSE ()
        STRING(SUBSTRING "${taosbenchmark_COMMIT_SHA1}" 0 7 taosbenchmark_COMMIT_SHA1)
        STRING(STRIP "${taosbenchmark_COMMIT_SHA1}" taosbenchmark_COMMIT_SHA1)
    ENDIF ()
    EXECUTE_PROCESS(
            COMMAND ${GIT_EXECUTABLE} describe --tags
            WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}
            RESULT_VARIABLE TAG_RESULT
            OUTPUT_VARIABLE TAOSTOOLS_TAG
    )
    MESSAGE("Taos tools tag: ${TAOSTOOLS_TAG}")
    IF ("${TAOSTOOLS_TAG}" STREQUAL "")
        SET(TAOSTOOLS_TAG "0.1.0")
    ELSE ()
        STRING(STRIP "${TAOSTOOLS_TAG}" TAOSTOOLS_TAG)
    ENDIF ()

    EXECUTE_PROCESS(
            COMMAND ${GIT_EXECUTABLE} status -z -s ${CMAKE_CURRENT_LIST_DIR}/taosdump.c
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE TAOSDUMP_STATUS
    )
    EXECUTE_PROCESS(
            COMMAND ${GIT_EXECUTABLE} status -z -s ${CMAKE_CURRENT_LIST_DIR}/bench*.c
            RESULT_VARIABLE RESULT
            OUTPUT_VARIABLE taosbenchmark_STATUS
    )
    IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
        EXECUTE_PROCESS(
                COMMAND bash "-c" "echo '${TAOSDUMP_STATUS}' | awk '{print $1}'"
                RESULT_VARIABLE RESULT
                OUTPUT_VARIABLE TAOSDUMP_STATUS
        )
        EXECUTE_PROCESS(
                COMMAND bash "-c" "echo '${taosbenchmark_STATUS}' | awk '{print $1}'"
                RESULT_VARIABLE RESULT
                OUTPUT_VARIABLE taosbenchmark_STATUS
        )
    ENDIF ()
ELSE()
    MESSAGE("Git not found")
    SET(TAOSDUMP_COMMIT_SHA1 "unknown")
    SET(taosbenchmark_COMMIT_SHA1 "unknown")
    SET(TAOSTOOLS_TAG "0.1.0")
    SET(TAOSDUMP_STATUS "unknown")
    SET(taosbenchmark_STATUS "unknown")
ENDIF (GIT_FOUND)

MESSAGE("taos-tools tag:" ${TAOSTOOLS_TAG})
MESSAGE("taosdump's latest commit in short is:" ${TAOSDUMP_COMMIT_SHA1})
MESSAGE("taosbenchmark's latest commit in short is:" ${taosbenchmark_COMMIT_SHA1})
STRING(STRIP "${TAOSDUMP_STATUS}" TAOSDUMP_STATUS)
STRING(STRIP "${taosbenchmark_STATUS}" taosbenchmark_STATUS)

IF (TAOSDUMP_STATUS MATCHES "M")
    SET(TAOSDUMP_STATUS "modified")
ELSE()
    SET(TAOSDUMP_STATUS "")
ENDIF ()

IF (taosbenchmark_STATUS MATCHES "M")
    SET(taosbenchmark_STATUS "modified")
ELSE()
    SET(taosbenchmark_STATUS "")
ENDIF ()

MESSAGE("taosdump's status is:" ${TAOSDUMP_STATUS})
MESSAGE("taosbenchmark's status is:" ${taosbenchmark_STATUS})

ADD_DEFINITIONS(-DTAOSTOOLS_TAG="${TAOSTOOLS_TAG}")
ADD_DEFINITIONS(-DTAOSDUMP_COMMIT_SHA1="${TAOSDUMP_COMMIT_SHA1}")
ADD_DEFINITIONS(-DTAOSDUMP_STATUS="${TAOSDUMP_STATUS}")
ADD_DEFINITIONS(-Dtaosbenchmark_COMMIT_SHA1="${taosbenchmark_COMMIT_SHA1}")
ADD_DEFINITIONS(-Dtaosbenchmark_STATUS="${taosbenchmark_STATUS}")

LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/build/lib ${CMAKE_BINARY_DIR}/build/lib64)
LINK_DIRECTORIES(/usr/lib /usr/lib64)
INCLUDE_DIRECTORIES(/usr/local/taos/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/avro/lang/c/src)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/toolscJson/src)

if(NOT DEFINED TD_VER_COMPATIBLE)
    SET(TD_VER_COMPATIBLE "2.0.0.0")
ENDIF()

IF (${TD_VER_COMPATIBLE} STRGREATER_EQUAL "3.0.0.0")
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/contrib/pthread)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/contrib/msvcregex)

    ADD_DEFINITIONS(-DTDENGINE_3)
ENDIF ()

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    SET(OS_ID "")

    IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
        ADD_DEFINITIONS(-DLINUX)

        EXECUTE_PROCESS (
                COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
                OUTPUT_VARIABLE OS_ID
        )
        MESSAGE(STATUS "src cmake LN147, detected The current OS is " ${OS_ID} "")

        ADD_LIBRARY(jansson STATIC IMPORTED)
        SET_PROPERTY(TARGET jansson PROPERTY IMPORTED_LOCATION
            "${CMAKE_BINARY_DIR}/build/lib/libjansson.a")

        ADD_LIBRARY(snappy STATIC IMPORTED)
        IF(${OS_ID} MATCHES "(CentOS)|(centos)" OR ${OS_ID} MATCHES "kylin"
                OR ${OS_ID} MATCHES "rhel" OR ${OS_ID} MATCHES "rocky")
            SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION
                "${CMAKE_BINARY_DIR}/build/lib64/libsnappy.a")
        ELSE()
            SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION
                "${CMAKE_BINARY_DIR}/build/lib/libsnappy.a")
        ENDIF ()

        ADD_EXECUTABLE(taosdump taosdump.c toolstime.c)
        ADD_DEPENDENCIES(deps-snappy apache-avro)
        ADD_DEPENDENCIES(taosdump deps-jansson)
        ADD_DEPENDENCIES(taosdump deps-snappy)
        IF (${TD_VER_COMPATIBLE} STRGREATER_EQUAL "3.0.0.0")
            ADD_EXECUTABLE(taosBenchmark benchMain.c benchTmq.c benchQuery.c benchJsonOpt.c benchInsert.c benchData.c benchCommandOpt.c benchUtil.c toolstime.c)
        ELSE()
            ADD_EXECUTABLE(taosBenchmark benchMain.c benchSubscribe.c benchQuery.c benchJsonOpt.c benchInsert.c benchData.c benchCommandOpt.c benchUtil.c toolstime.c)
        ENDIF()
    ELSE ()
        IF (${TD_VER_COMPATIBLE} STRGREATER_EQUAL "3.0.0.0")
            ADD_EXECUTABLE(taosBenchmark benchMain.c benchTmq.c benchQuery.c benchJsonOpt.c benchInsert.c benchData.c benchCommandOpt.c benchUtil.c toolstime.c)
        ELSE()
            ADD_EXECUTABLE(taosBenchmark benchMain.c benchSubscribe.c benchQuery.c benchJsonOpt.c benchInsert.c benchData.c benchCommandOpt.c benchUtil.c toolstime.c)
        ENDIF()
        INCLUDE_DIRECTORIES(/usr/local/include)
        ADD_DEFINITIONS(-DDARWIN)
        SET(OS_ID "Darwin")
    ENDIF ()

    IF (${WEBSOCKET})
        INCLUDE_DIRECTORIES(/usr/local/include/)
        SET(WEBSOCKET_LINK_FLAGS "-ltaosws")
        ADD_DEPENDENCIES(taosBenchmark taosws-rs)
        ADD_DEPENDENCIES(taosdump taosws-rs)
    ELSE ()
        SET(WEBSOCKET_LINK_FLAGS "")
    ENDIF ()

    IF (${TD_VER_COMPATIBLE} STRGREATER_EQUAL "3.0.0.0")
        SET(OS_LINK_FLAGS "os")
    ELSE()
        SET("")
    ENDIF()

    IF (${COVER} MATCHES "true")
        MESSAGE("${Green} build with coverage ${ColourReset}")
        SET(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
        SET(GCC_COVERAGE_LINK_FLAGS    "-lgcov --coverage")
    ENDIF ()

    IF (${TOOLS_BUILD_TYPE} MATCHES "Debug")
        IF (${TOOLS_SANITIZE} MATCHES "true")
            MESSAGE("${Yellow} Enable memory sanitize ${ColourReset}")
            IF (${OS_ID} MATCHES "Darwin")
                SET(TOOLS_SANITIZE_FLAG "-static-libsan -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
            ELSE ()
                SET(TOOLS_SANITIZE_FLAG "-static-libasan -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment")
            ENDIF ()
        ELSE ()
            SET(TOOLS_SANITIZE_FLAG "")
        ENDIF ()

        IF(CMAKE_C_COMPILER_VERSION VERSION_GREATER 4.8.5)
            MESSAGE("${Green} GCC version is ${CMAKE_C_COMPILER_VERSION}${ColourReset}")
            SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 ${TOOLS_SANITIZE_FLAG} -O0 -g3 -DDEBUG0ww0 ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS} ${WEBSOCKET_LINK_FLAGS}")
        ELSE()
            MESSAGE("${Yellow} GCC version is equal or less than 4.8.5, which does not support memory sanitizer. Please upgrade to a higher version if you want to use it. ${ColourReset}")
            SET(CMAKE_C_FLAGS "-std=c99 -O0 -g3 -DDEBUG ${WEBSOCKET_LINK_FLAGS}")
        ENDIF()

        IF (${OS_ID} MATCHES "alpine")
            MESSAGE("${Yellow} DEBUG mode use shared avro library to link for debug ${ColourReset}")
            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread toolscJson ${OS_LINK_FLAGS} ${WEBSOCKET_LINK_FLAGS})
            TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread argp ${WEBSOCKET_LINK_FLAGS})
        ELSEIF(${OS_ID} MATCHES "Darwin")
            ADD_LIBRARY(argp STATIC IMPORTED)
            IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/argp-standalone/lib/libargp.a")
                INCLUDE_DIRECTORIES(/opt/homebrew/opt/argp-standalone/include/)
            ELSE ()
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libargp.a")
                INCLUDE_DIRECTORIES(/usr/local/include/include/)
            ENDIF ()
            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread toolscJson argp ${OS_LINK_FLAGS} ${WEBSOCKET_LINK_FLAGS})
        ElSE ()
            MESSAGE("${Yellow} DEBUG mode use shared avro library to link for debug ${ColourReset}")
            TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread ${WEBSOCKET_LINK_FLAGS})
            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread toolscJson ${WEBSOCKET_LINK_FLAGS})
        ENDIF()

    ELSE ()
        MESSAGE("${Green} RELEASE mode use static avro library to link for release ${ColourReset}")
        SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -DRELEASE ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")

        IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
            #            ADD_LIBRARY(jansson STATIC IMPORTED)
            #SET_PROPERTY(TARGET jansson PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/jansson/lib/libjansson.a")

            # ADD_LIBRARY(snappy STATIC IMPORTED)
            # SET_PROPERTY(TARGET snappy PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/snappy/lib/libsnappy.a")

            # ADD_LIBRARY(avro STATIC IMPORTED)
            # SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/avro-c/lib/libavro.a")
            #            TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy lzma z pthread)
            ADD_LIBRARY(argp STATIC IMPORTED)
            IF (CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/opt/homebrew/opt/argp-standalone/lib/libargp.a")
                INCLUDE_DIRECTORIES(/opt/homebrew/opt/argp-standalone/include/)
            ELSE ()
                SET_PROPERTY(TARGET argp PROPERTY IMPORTED_LOCATION "/usr/local/lib/libargp.a")
                INCLUDE_DIRECTORIES(/usr/local/include/)
            ENDIF ()

            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread toolscJson argp ${OS_LINK_FLAGS} ${WEBSOCKET_LINK_FLAGS})
        ELSE ()
            EXECUTE_PROCESS (
                COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
                OUTPUT_VARIABLE OS_ID
            )
            MESSAGE(STATUS "src cmake LN303, detected The current OS is " ${OS_ID} "")

            ADD_LIBRARY(avro STATIC IMPORTED)
            IF(${OS_ID} MATCHES "(CentOS)|(centos)" OR ${OS_ID} MATCHES "kylin"
                    OR ${OS_ID} MATCHES "rhel" OR ${OS_ID} MATCHES "rocky")
                SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION
                    "${CMAKE_BINARY_DIR}/build/lib64/libavro.a")
            ELSE()
                SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION
                    "${CMAKE_BINARY_DIR}/build/lib/libavro.a")
            ENDIF()

            EXECUTE_PROCESS (
                    COMMAND gcc -print-file-name=libstdc++.a
                    OUTPUT_VARIABLE LIBSTDCXX_PATH
            )

            STRING(STRIP "${LIBSTDCXX_PATH}" LIBSTDCXX_PATH)
            MESSAGE("libstdc++.a path:" ${LIBSTDCXX_PATH})
            ADD_LIBRARY(stdc++ STATIC IMPORTED)
            SET_PROPERTY(TARGET stdc++ PROPERTY IMPORTED_LOCATION "${LIBSTDCXX_PATH}")

            IF (${OS_ID} MATCHES "alpine")
                TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma z atomic pthread argp ${WEBSOCKET_LINK_FLAGS})
            ELSE()
                TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma z atomic pthread ${WEBSOCKET_LINK_FLAGS})
            ENDIF()
            TARGET_LINK_LIBRARIES(taosBenchmark taos pthread toolscJson ${OS_LINK_FLAGS} ${WEBSOCKET_LINK_FLAGS})
        ENDIF ()

    ENDIF ()
ELSE ()
    set (CMAKE_C_STANDARD 11)
    SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /utf-8")
    ADD_EXECUTABLE(taosBenchmark benchMain.c benchSubscribe.c benchQuery.c benchJsonOpt.c benchInsert.c benchData.c benchCommandOpt.c benchUtil.c toolstime.c)

    IF (${WEBSOCKET})
        INCLUDE_DIRECTORIES(/usr/local/include/)
        SET(WEBSOCKET_LINK_FLAGS "-ltaosws")
        ADD_DEPENDENCIES(taosBenchmark taosws-rs)
        #ADD_DEPENDENCIES(taosdump taosws-rs)
    ELSE ()
        SET(WEBSOCKET_LINK_FLAGS "")
    ENDIF ()

    TARGET_LINK_LIBRARIES(taosBenchmark taos_static pthread toolscJson ${OS_LINK_FLAGS} ${WEBSOCKET_LINK_FLAGS})
ENDIF ()
