#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <iostream>
#include <chrono>
#include <atomic>
#include <thread>
#include <mutex>
#include "sim_server.h"

#define LEN (1 * 1024 * 1024L)

std::atomic<int64_t> g_pkts;
std::atomic<int64_t> g_pkts_length;
SimulatedServer *g_sim_server;

extern void accept_loop(std::string port);
extern void receive_loop(int new_socket);
extern void print_statistics();
void dmclock_reschedule(const MsgHeader& header, uint8_t* resp_phase_type, uint32_t* resp_cost);

void accept_loop(std::string port)
{
  int server_fd, new_socket;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);
  
  // Creating socket file descriptor
  if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
  {
    std::cout << "socket failed, "
	      << strerror(errno)
	      << std::endl;
    return;
  }
  
  // Forcefully attaching socket to the port
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
		 &opt, sizeof(opt)))
  {
    std::cout << "setsockopt, "
	      << strerror(errno)
	      << std::endl;
  }
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(std::stoi(port));
  
  // Forcefully attaching socket to the port
  if (bind(server_fd, (struct sockaddr *)&address,
	   sizeof(address)) < 0)
  {
    close(server_fd);
    std::cout << "bind failed, "
	      << strerror(errno)
	      << std::endl;
    return;
  }
  if (listen(server_fd, 3) < 0)
  {
    close(server_fd);
    std::cout << "listen, "
	      << strerror(errno)
	      << std::endl;
    return;
  }
  
  while (true)
  {
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address,
			    (socklen_t*)&addrlen)) < 0)
    {
      close(server_fd);
      std::cout << "accept, "
		<< strerror(errno)
		<< std::endl;
      return;
    }
    int buff_size = (16 * 1024 * 1024);
    setsockopt(new_socket, SOL_SOCKET, SO_RCVBUF, &buff_size, sizeof(buff_size));
    
    std::thread thd = std::thread(receive_loop, new_socket);
    std::cout << "New worker, thread = "
	      << thd.get_id()
	      << std::endl;
    thd.detach();
  }
}

void receive_loop(int new_socket)
{
  char buffer[LEN];
  
  while (true)
  {
    MsgHeader header;
    char *ptr = (char *)&header;
    uint32_t len = sizeof(MsgHeader);
    int ret = 0;
    // Receive header
    while (len != 0)
    {
      ptr = ptr + sizeof(MsgHeader) - len;
      ret = read(new_socket, ptr, len);
      if (ret <= 0)
      {
	close(new_socket);
	std::cout << "Worker exit while reading header, thread = "
		  << std::this_thread::get_id()
		  << ", ret = "
		  << ret
		  << std::endl;
	return;
      }
      len -= ret;
    }
  
    // std::cout << "payload_length = " << header.payload_length << std::endl;
    assert(LEN >= header.payload_length);

    // Receive payload
    len = header.payload_length;
    while (len != 0)
    {
      ret = read(new_socket, buffer, len);
      if (ret <= 0)
      {
	close(new_socket);
	std::cout << "Worker exit while reading payload, thread = "
		  << std::this_thread::get_id()
		  << ", ret = "
		  << ret
		  << std::endl;
	return;
      }
      len -= ret;
    }
  
    g_pkts++;
    g_pkts_length += header.payload_length;
    // Dmclock reschedule
    uint8_t resp_phase_type = 0;
    uint32_t resp_cost = 0;
    uint32_t resp_length = header.payload_length;
    dmclock_reschedule(header, &resp_phase_type, &resp_cost);
    
    // Send response
    memset(&header, 0, sizeof(MsgHeader));
    header.payload_length = 0;
    header.req_delta = 0;
    header.req_rho = 0;
    header.req_cost = 0;
    header.req_lambda = 0;
    header.resp_phase_type = resp_phase_type;
    header.resp_cost = resp_cost;
    header.resp_length = resp_length;
    ret = send(new_socket, &header, sizeof(MsgHeader), 0 );
    if (ret < 0)
    {
      std::cout << "Worker exit while sendding header, thread = "
		<< std::this_thread::get_id()
		<< ", ret = "
		<< ret
		<< ", "
		<< strerror(errno)
		<< std::endl;
      break;
    }
    if (ret != sizeof(MsgHeader))
    {
      std::cout << "Worker exit while sending header, thread = "
		<< std::this_thread::get_id()
		<< ", ret = "
		<< ret
		<< std::endl;
    }

  }
}

void print_statistics()
{
  while (true)
  {
    int64_t start = std::chrono::duration_cast<std::chrono::seconds>(
			    std::chrono::system_clock::now().time_since_epoch()).count();

    sleep(5);

    int64_t end = std::chrono::duration_cast<std::chrono::seconds>(
			    std::chrono::system_clock::now().time_since_epoch()).count();

    int64_t pkts = 0;
    int64_t pkts_length = 0;
    {
      pkts = g_pkts;
      pkts_length = g_pkts_length;
      g_pkts = 0;
      g_pkts_length = 0;
    }
    std::cout << "start = "
	      << start
	      << ", end = "
	      << end 
	      << ", qps = "
	      << pkts / (end - start)
	      << "/s, bandwidth = "
	      << (pkts_length) / 1024 / 1024 / (end - start)
	      << " MiB/s"
	      << std::endl;
  }
}

void dmclock_reschedule(const MsgHeader& header, uint8_t* resp_phase_type, uint32_t* resp_cost)
{
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  double get_time = now.tv_sec + (now.tv_nsec / 1.0e9);

  uint32_t delta = header.req_delta;
  uint32_t rho = header.req_rho;
  uint32_t cost = header.req_cost;
  uint64_t lambda = header.req_lambda + header.payload_length;
  ClientId client_id = header.client_id;
  
  std::mutex reschedule_mutex;
  std::condition_variable reschedule_cv;
  bool reschedule_done = false;

  ClientRequest *request_raw_ptr = new ClientRequest();
  std::unique_ptr<ClientRequest> request(request_raw_ptr);
  request->reschedule_mutex_ptr = &reschedule_mutex;
  request->reschedule_cv_ptr = &reschedule_cv;
  request->reschedule_done = &reschedule_done;
  request->resp_phase_type = resp_phase_type;
  request->resp_cost = resp_cost;
  
  // lose the control of request
  g_sim_server->post(std::move(request), client_id, {delta, rho, lambda}, cost, get_time);
  
  std::unique_lock<std::mutex> l(reschedule_mutex);
  while (!reschedule_done)
  {
    reschedule_cv.wait(l);
  }

  // got the control of request again
  request.reset(request_raw_ptr);
}

int main(int argc, char const *argv[])
{
  if (argc != 2)
  {
    printf("Usage: socket_sim_server <port number>\n");
    return -1; 
  }

  g_pkts = 0;
  g_sim_server = new SimulatedServer(5);
  std::unique_ptr<SimulatedServer> server(g_sim_server);
  #if 1
  server->add_client_info(12345, ClientInfo(100.0, 1.0, 3000.0, 55.0 * 1024.0 * 1024.0));
  #else
  server->add_client_info(12345, ClientInfo(100.0, 1.0, 4000.0, 55.0 * 1024.0 * 1024.0));
  #endif

  std::thread thd_stat = std::thread(print_statistics);
  thd_stat.detach();

  std::string port = argv[1];  
  accept_loop(port);

  return 0;
}

