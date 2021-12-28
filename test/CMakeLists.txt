INCLUDE_DIRECTORIES(../inc)
SET(CMAKE_C_FLAGS " -O0 -g3")
ADD_EXECUTABLE(taosdemoTest taosdemoTest.c)
target_link_libraries(taosdemoTest cunit taos pthread cJsonDemo taosdemo)
