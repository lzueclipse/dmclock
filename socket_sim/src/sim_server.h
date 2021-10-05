#pragma once

#include <assert.h>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <deque>

#include <dmclock_recs.h>
#include <dmclock_server.h>

struct MsgHeader {
  uint32_t payload_length;

  uint32_t req_delta;
  uint32_t req_rho;
  uint32_t req_cost;
  uint32_t req_lambda;
  
  uint32_t resp_phase_type;
  uint32_t resp_cost;
  uint64_t resp_length;
};

struct ClientRequest {
  std::mutex *reschedule_mutex_ptr;
  std::condition_variable *reschedule_cv_ptr;
  bool *reschedule_done;
  uint8_t *resp_phase_type;
  uint32_t *resp_cost;
};

using crimson::dmclock::ClientInfo;
using crimson::dmclock::AtLimit;
using crimson::dmclock::PhaseType;
using crimson::dmclock::ReqParams;
using crimson::dmclock::Cost;

using ClientId = std::string;
using DmcQueue = crimson::dmclock::PushPriorityQueue<ClientId, ClientRequest, true, false, 3>;

struct QueueItem {
  ClientId                       client;
  std::unique_ptr<ClientRequest> request;
  PhaseType                      phase_type;
  Cost                           request_cost;

  QueueItem(const ClientId&                  _client,
	    std::unique_ptr<ClientRequest>&& _request,
	    const PhaseType&                 _phase_type,
	    const Cost                       _request_cost) :
    client(_client),
    request(std::move(_request)),
    phase_type(_phase_type),
    request_cost(_request_cost)
  {
    // empty
  }
}; // QueueItem


class SimulatedServer {
protected:
  size_t                         thread_pool_size;
  bool                           finishing;
  DmcQueue*                      priority_queue;

  std::mutex                     inner_queue_mtx;
  std::condition_variable        inner_queue_cv;
  std::deque<QueueItem>          inner_queue;

  std::thread*                   threads;

  // demo only, no update, no lock
  std::map<ClientId, ClientInfo> client_info_map;

  using InnerQGuard = std::lock_guard<decltype(inner_queue_mtx)>;
  using Lock = std::unique_lock<std::mutex>;

public:
  SimulatedServer(size_t _thread_pool_size)
    : thread_pool_size(_thread_pool_size),
      finishing(false)
  {
    priority_queue = new DmcQueue(
				   std::bind(&SimulatedServer::get_client_info,
					     this,
					     std::placeholders::_1),
				   std::bind(&SimulatedServer::has_avail_thread,
					     this),
				   std::bind(&SimulatedServer::inner_post,
					     this,
					     std::placeholders::_1,
					     std::placeholders::_2,
					     std::placeholders::_3,
					     std::placeholders::_4),
				   crimson::dmclock::AtLimit::Wait);
    std::chrono::milliseconds finishing_check_period(1000);
    threads = new std::thread[thread_pool_size];
    for (size_t i = 0; i < thread_pool_size; ++i) {
      threads[i] = std::thread(&SimulatedServer::run, this, finishing_check_period);
    }
  }

  virtual ~SimulatedServer() {
    Lock l(inner_queue_mtx);
    finishing = true;
    inner_queue_cv.notify_all();
    l.unlock();

    for (size_t i = 0; i < thread_pool_size; ++i) {
      threads[i].join();
    }

    delete[] threads;

    delete priority_queue;
  }

  const ClientInfo* get_client_info(const ClientId& client_id)
  {
    //demo only, no lock, no update
    ClientInfo* ci = nullptr;
    auto it = client_info_map.find(client_id);
    if (it != client_info_map.end())
    {
      ci = &(it->second);
    }
    else
    {
      ci = nullptr;
    }
    return ci;
  }

  void add_client_info(const ClientId& client_id, const ClientInfo& client_info)
  {
    //demo only, no lock, no update
    client_info_map.insert(std::make_pair(client_id, client_info));
  }

  void post(std::unique_ptr<ClientRequest> request,
	    const ClientId& client_id,
	    const ReqParams& req_params,
	    const Cost request_cost,
	    const double get_time)
  {
    priority_queue->add_request(std::move(request),
				client_id,
				req_params,
				get_time,
				request_cost);
  }

  bool has_avail_thread() {
    // InnerQGuard g(inner_queue_mtx);
    // return inner_queue.size() <= thread_pool_size;
    return true;
  }

  const DmcQueue* get_priority_queue() const { return priority_queue; }

protected:

  void inner_post(const ClientId& client,
		  std::unique_ptr<ClientRequest> request,
		  const PhaseType& phase_type,
		  const Cost request_cost) {
    Lock l(inner_queue_mtx);
    assert(!finishing);
    inner_queue.emplace_back(QueueItem(client,
				       std::move(request),
				       phase_type,
				       request_cost));
    inner_queue_cv.notify_one();
  }

  void run(std::chrono::milliseconds check_period) {
    Lock l(inner_queue_mtx);
    while(true) {
      while(inner_queue.empty() && !finishing) {
	inner_queue_cv.wait_for(l, check_period);
      }
      if (!inner_queue.empty()) {
	auto& front = inner_queue.front();
	// auto client = front.client;
	auto req = std::move(front.request);
	auto phase_type = front.phase_type;
	auto request_cost = front.request_cost;
	inner_queue.pop_front();

	l.unlock();

	Lock reqL(*(req->reschedule_mutex_ptr));
	*(req->reschedule_done) = true;
	*(req->resp_phase_type) = (uint8_t) phase_type;
	*(req->resp_cost) = request_cost;
	req->reschedule_cv_ptr->notify_one();
	req.release();
	reqL.lock();

	priority_queue->request_completed();

	l.lock(); // in prep for next iteration of loop
      } else {
	break;
      }
    }
  }
}; // class SimulatedServer

