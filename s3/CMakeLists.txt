if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

include_directories(${CMAKE_BINARY_DIR}/third_party/libs3/include)
add_library(cfunctions SHARED cfunctions.c)
target_link_libraries(cfunctions ${MSGPUCK_LIBRARIES} ${CMAKE_BINARY_DIR}/third_party/libs3/lib/libs3.a -lcurl -lxml2 -lpthread -lssl -lcrypto )
set_target_properties(cfunctions PROPERTIES PREFIX "" OUTPUT_NAME "cfunctions")

# Install module
install(FILES init.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/${PROJECT_NAME}/)
install(TARGETS cfunctions LIBRARY DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/${PROJECT_NAME}/)
