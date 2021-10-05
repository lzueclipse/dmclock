
// Client side C/C++ program to demonstrate Socket programming
#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <chrono>
#include <atomic>
#include <thread>
#include <mutex>
#include <vector>

#define LEN 2048

std::atomic<int64_t> g_pkts;
bool g_quit = false;

void send_loop(std::string port)
{
  int sock = 0;
  struct sockaddr_in serv_addr;
  char buffer[LEN] = {'a'};

  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
    std::cout << "Socket creation error, "
	      << strerror(errno)
              << std::endl;
    return; 
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(std::stoi(port));

  // Convert IPv4 and IPv6 addresses from text to binary form
  if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr)<=0)
  {
    std::cout << "Invalid address/ Address not supported, "
	      << strerror(errno)
              << std::endl;
    return;
  }

  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
  {
    std::cout << "Connection Failed, "
	      << strerror(errno)
              << std::endl;;
    return;
  }

  int buff_size = (16 * 1024 * 1024);
  setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buff_size, sizeof(buff_size)); 

  while (true)
  {
    int ret = send(sock, buffer, LEN, 0 );
    if (ret < 0)
    {
      std::cout << "Exit, ret = "
		<< ret
		<< ", "
		<< strerror(errno)
		<< std::endl;
      g_quit = true;
      break;
    }
    if (ret != LEN)
    {
      std::cout << "Exit, ret = "
		<< ret
		<< std::endl;
    }
    {
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
  if (argc < 2)
  {
    printf("Usage: socket_sim_client <port number> <port number> ... <port number>\n");
    return -1; 
  }

  g_pkts = 0;
  std::thread thd_stat = std::thread(print_statistics);
  
  std::vector<std::thread> thd_vec;
  for (int i = 1; i < argc; i++)
  { 
    thd_vec.push_back(std::thread(send_loop, argv[i]));
  }

  for (auto& thd : thd_vec)
  {
    thd.join();
  }

  thd_stat.join();
  return 0;
}
