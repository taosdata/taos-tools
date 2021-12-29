INCLUDE_DIRECTORIES(../inc)
SET(CMAKE_C_FLAGS " -O0 -g3")
ADD_EXECUTABLE(benchTest benchTest.c)
target_link_libraries(benchTest cunit taos pthread cJsonDemo benchmark)
