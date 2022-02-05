CMAKE_MINIMUM_REQUIRED(VERSION 3.0...3.20)

INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
INCLUDE_DIRECTORIES(../inc)

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
            COMMAND ${GIT_EXECUTABLE} status -z -s ${CMAKE_CURRENT_LIST_DIR}/demo*.c
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
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/cJsonDemo/src)

IF (${COVER} MATCHES "true")
    MESSAGE("${Green} build with coverage ${ColourReSET}")
    SET(GCC_COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage")
    SET(GCC_COVERAGE_LINK_FLAGS    "-lgcov --coverage")
ENDIF ()

IF (${BUILD_TYPE} MATCHES "Debug")
    MESSAGE("${Yellow} DEBUG mode ${ColourReset}")
    SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -static-libasan -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment -O0 -g3 -DDEBUG0ww0 ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")
ELSE ()
    MESSAGE("${Green} RELEASE mode ${ColourReset}")
    SET(CMAKE_C_FLAGS "-Wall -Wshadow -std=c99 -std=gnu11 -DRELEASE ${GCC_COVERAGE_COMPILE_FLAGS} ${GCC_COVERAGE_LINK_FLAGS}")
ENDIF ()

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    SET(OS_ID "")
    ADD_EXECUTABLE(taosdump taosdump.c)
    ADD_EXECUTABLE(taosBenchmark benchMain.c benchSubscribe.c benchQuery.c benchOutput.c benchJsonOpt.c benchInsert.c benchData.c benchCommandOpt.c benchUtil.c)
    execute_process (
        COMMAND sh -c "awk -F= '/^ID=/{print $2}' /etc/os-release |tr -d '\n' | tr -d '\"'"
        OUTPUT_VARIABLE OS_ID
    )
    MESSAGE(DEBUG "Linux os ID: ${OD_ID}")

    ADD_LIBRARY(avro STATIC IMPORTED)
    SET_PROPERTY(TARGET avro PROPERTY IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/build/lib/libavro.a")
    IF (${OS_ID} MATCHES "alpine")
        TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma z atomic pthread argp)
    ELSE()
        TARGET_LINK_LIBRARIES(taosdump taos avro jansson snappy stdc++ lzma z atomic pthread)
    ENDIF()

    target_link_libraries(taosBenchmark taos pthread cJsonDemo)

ENDIF ()

