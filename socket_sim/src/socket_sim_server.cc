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

extern void accept_loop(std::string port);
extern void receive_loop(int new_socket);
extern void print_statistics();
void dmclock_reschedule();

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
  MsgHeader header;
  char buffer[LEN];
  while (true)
  {
    char *ptr = (char *)&header;
    uint32_t len = sizeof(MsgHeader);
    int ret = 0;
    while (len != 0)
    {
      ret = read(new_socket, ptr + sizeof(MsgHeader) - len, len);
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

void dmclock_reschedule()
{
  
}

int main(int argc, char const *argv[])
{
  if (argc != 2)
  {
    printf("Usage: socket_sim_server <port number>\n");
    return -1; 
  }

  g_pkts = 0;
  std::thread thd_stat = std::thread(print_statistics);
  thd_stat.detach();

  std::string port = argv[1];  
  accept_loop(port);

  return 0;
}

