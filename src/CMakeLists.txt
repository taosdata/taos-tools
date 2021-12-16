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
            OUTPUT_VARIABLE TAOSDEMO_COMMIT_SHA1
    )
    IF ("${TAOSDUMP_COMMIT_SHA1}" STREQUAL "")
        SET(TAOSDUMP_COMMIT_SHA1 "unknown")
    ELSE ()
        STRING(SUBSTRING "${TAOSDUMP_COMMIT_SHA1}" 0 7 TAOSDUMP_COMMIT_SHA1)
        STRING(STRIP "${TAOSDUMP_COMMIT_SHA1}" TAOSDUMP_COMMIT_SHA1)
    ENDIF ()
    IF ("${TAOSDEMO_COMMIT_SHA1}" STREQUAL "")
        SET(TAOSDEMO_COMMIT_SHA1 "unknown")
    ELSE ()
        STRING(SUBSTRING "${TAOSDEMO_COMMIT_SHA1}" 0 7 TAOSDEMO_COMMIT_SHA1)
        STRING(STRIP "${TAOSDEMO_COMMIT_SHA1}" TAOSDEMO_COMMIT_SHA1)
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
            OUTPUT_VARIABLE TAOSDEMO_STATUS
    )
    IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
        EXECUTE_PROCESS(
                COMMAND bash "-c" "echo '${TAOSDUMP_STATUS}' | awk '{print $1}'"
                RESULT_VARIABLE RESULT
                OUTPUT_VARIABLE TAOSDUMP_STATUS
        )
        EXECUTE_PROCESS(
                COMMAND bash "-c" "echo '${TAOSDEMO_STATUS}' | awk '{print $1}'"
                RESULT_VARIABLE RESULT
                OUTPUT_VARIABLE TAOSDEMO_STATUS
        )
    ENDIF ()
ELSE()
    MESSAGE("Git not found")
    SET(TAOSDUMP_COMMIT_SHA1 "unknown")
    SET(TAOSDEMO_COMMIT_SHA1 "unknown")
    SET(TAOSTOOLS_TAG "0.1.0")
    SET(TAOSDUMP_STATUS "unknown")
    SET(TAOSDEMO_STATUS "unknown")
ENDIF (GIT_FOUND)

MESSAGE("taos-tools tag:" ${TAOSTOOLS_TAG})
MESSAGE("taosdump's latest commit in short is:" ${TAOSDUMP_COMMIT_SHA1})
MESSAGE("taosdemo's latest commit in short is:" ${TAOSDEMO_COMMIT_SHA1})
STRING(STRIP "${TAOSDUMP_STATUS}" TAOSDUMP_STATUS)
STRING(STRIP "${TAOSDEMO_STATUS}" TAOSDEMO_STATUS)

IF (TAOSDUMP_STATUS MATCHES "M")
    SET(TAOSDUMP_STATUS "modified")
ELSE()
    SET(TAOSDUMP_STATUS "")
ENDIF ()

IF (TAOSDEMO_STATUS MATCHES "M")
    SET(TAOSDEMO_STATUS "modified")
ELSE()
    SET(TAOSDEMO_STATUS "")
ENDIF ()

MESSAGE("taosdump's status is:" ${TAOSDUMP_STATUS})
MESSAGE("taosdemo's status is:" ${TAOSDEMO_STATUS})

ADD_DEFINITIONS(-DTAOSTOOLS_TAG="${TAOSTOOLS_TAG}")
ADD_DEFINITIONS(-DTAOSDUMP_COMMIT_SHA1="${TAOSDUMP_COMMIT_SHA1}")
ADD_DEFINITIONS(-DTAOSDUMP_STATUS="${TAOSDUMP_STATUS}")
ADD_DEFINITIONS(-DTAOSDEMO_COMMIT_SHA1="${TAOSDEMO_COMMIT_SHA1}")
ADD_DEFINITIONS(-DTAOSDEMO_STATUS="${TAOSDEMO_STATUS}")

LINK_DIRECTORIES(${CMAKE_BINARY_DIR}/build/lib ${CMAKE_BINARY_DIR}/build/lib64)
LINK_DIRECTORIES(/usr/lib /usr/lib64)
INCLUDE_DIRECTORIES(/usr/local/taos/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/avro/lang/c/src)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_LIST_DIR}/../deps/cJsonDemo/src)

IF (${CMAKE_BUILD_TYPE} MATCHES "Debug")
    MESSAGE("${Yellow} DEBUG mode ${ColourReset}")
    SET(CMAKE_C_FLAGS "-std=c99 -static-libasan -fsanitize=address -fsanitize=undefined -fno-sanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=null -fno-sanitize=alignment -O0 -g3 -DDEBUG0ww0")
ELSE ()
    MESSAGE("${Green} RELEASE mode ${ColourReset}")
    SET(CMAKE_C_FLAGS "-std=c99 -DRELEASE")
ENDIF ()

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    ADD_EXECUTABLE(taosdump taosdump.c)
    ADD_LIBRARY(taosdemo SHARED demoMain.c demoSubscribe.c demoQuery.c demoOutput.c demoJsonOpt.c demoInsert.c demoData.c demoCommandOpt.c demoUtil.c)
    ADD_EXECUTABLE(taosBenchmark demoMain.c)
    TARGET_LINK_LIBRARIES(taosdump taos avro jansson atomic pthread)
    target_link_libraries(taosdemo taos pthread cJsonDemo -fsanitize=address)
    TARGET_LINK_LIBRARIES(taosBenchmark taosdemo)
ENDIF ()

