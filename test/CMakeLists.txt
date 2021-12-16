INCLUDE_DIRECTORIES(../inc)
ADD_EXECUTABLE(taosdemoTest taosdemoTest.c)
target_link_libraries(taosdemoTest cunit taos pthread cJsonDemo taosdemo)
