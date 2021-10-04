set(local_flags "-Wall ")

set(socket_sim_client_srcs socket_sim_client.cc)
set(socket_sim_server_srcs socket_sim_server.cc)

set_source_files_properties(${socket_sim_client_srcs} ${socket_sim_server_srcs} 
  PROPERTIES
  COMPILE_FLAGS "${local_flags}"
  )

set(warnings_off " -Wno-unused-but-set-variable -Wno-unused-function")

# append warning flags to certain source files
set_property(
  SOURCE ${socket_sim_client_srcs} ${socket_sim_server_srcs}
  APPEND_STRING
  PROPERTY COMPILE_FLAGS "${warnings_off}"
  )

include_directories(${PROJECT_SOURCE_DIR}/src)

add_executable(socket_sim_client ${socket_sim_client_srcs})
target_link_libraries(socket_sim_client
                      Threads::Threads
                      dmclock)

add_executable(socket_sim_server ${socket_sim_server_srcs})
target_link_libraries(socket_sim_server
                      Threads::Threads
                      dmclock)
