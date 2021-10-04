// Server side C/C++ program to demonstrate Socket programming
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

#define LEN  2048

std::mutex g_pkts_mtx;
int64_t g_pkts = 0;
bool g_quit = false;

void receive_loop(std::string port)
{
  int server_fd, new_socket;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);
  char buffer[LEN];
  
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
    std::cout << "bind failed, "
	      << strerror(errno)
	      << std::endl;
  }
  if (listen(server_fd, 3) < 0)
  {
    std::cout << "listen, "
	      << strerror(errno)
	      << std::endl;
    return;
  }
  if ((new_socket = accept(server_fd, (struct sockaddr *)&address,
			   (socklen_t*)&addrlen)) < 0)
  {
    std::cout << "accept, "
	      << strerror(errno)
	      << std::endl;
    return;
  }

  int buff_size = (16 * 1024 * 1024);
  setsockopt(new_socket, SOL_SOCKET, SO_RCVBUF, &buff_size, sizeof(buff_size));

  while (true)
  {
    int len = 0, ret = 0;
    while (len != LEN)
    {
      ret = read(new_socket, buffer, LEN);
      if (ret <= 0)
      {
	std::cout << "Exit, ret = "
		  << ret
		  << ", "
		  << strerror(errno)
		  << std::endl;
	g_quit = true;
	break;
      }
      len += ret;
    }
    if (len != LEN)
    {
      std::cout << "Exit, ret = "
		<< ret
		<< std::endl;
      g_quit = true;
      break;
    }
    {
      std::lock_guard<std::mutex> lock(g_pkts_mtx);
      g_pkts++;
    }
  }
}

void print_statistics()
{
  while (!g_quit)
  {
    int64_t start = std::chrono::duration_cast<std::chrono::seconds>(
			    std::chrono::system_clock::now().time_since_epoch()).count();

    sleep(5);

    int64_t end = std::chrono::duration_cast<std::chrono::seconds>(
			    std::chrono::system_clock::now().time_since_epoch()).count();

    int64_t pkts = 0;
    {
      std::lock_guard<std::mutex> lock(g_pkts_mtx);
      pkts = g_pkts;
      g_pkts = 0;
    }
    std::cout << "start = "
	      << start
	      << ", end = "
	      << end 
	      << ", qps = "
	      << pkts / (end - start)
	      << "/s, bandwidth = "
	      << (pkts * LEN) / 1024 / 1024 / (end - start)
	      << " MiB/s"
	      << std::endl;
  }
}


int main(int argc, char const *argv[])
{
  if (argc != 2)
  {
    printf("Usage: socket_sim_server <port number>\n");
    return -1; 
  }

  std::thread thd_stat = std::thread(print_statistics);

  std::string port = argv[1];  
  receive_loop(port);

  thd_stat.join();
  return 0;
}

