#pragma once

#include <assert.h>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <deque>

template<typename Q, typename ReqPm, typename RespPm>
class SimulatedServer {

  struct QueueItem {
    ClientId                     client;
    std::unique_ptr<TestRequest> request;
    RespPm                       additional;
    Cost                         request_cost;

    QueueItem(const ClientId&                _client,
	      std::unique_ptr<TestRequest>&& _request,
	      const RespPm&                  _additional,
	      const Cost                     _request_cost) :
      client(_client),
      request(std::move(_request)),
      additional(_additional),
      request_cost(_request_cost)
    {
      // empty
    }
  }; // QueueItem

public:

  using ClientRespFunc = std::function<void(ClientId,
					    const TestResponse&,
					    const RespPm&,
					    const Cost)>;

protected:

  Q*                             priority_queue;
  ClientRespFunc                 client_resp_f;
  size_t                         thread_pool_size;

  bool                           finishing;

  std::mutex                     inner_queue_mtx;
  std::condition_variable        inner_queue_cv;
  std::deque<QueueItem>          inner_queue;

  std::thread*                   threads;

  using InnerQGuard = std::lock_guard<decltype(inner_queue_mtx)>;
  using Lock = std::unique_lock<std::mutex>;


public:

  using CanHandleRequestFunc = std::function<bool(void)>;
  using HandleRequestFunc =
    std::function<void(const ClientId&,std::unique_ptr<TestRequest>,const RespPm&, uint64_t)>;
  using CreateQueueF = std::function<Q*(CanHandleRequestFunc,HandleRequestFunc)>;
				    

  SimulatedServer(size_t _thread_pool_size,
		  const ClientRespFunc& _client_resp_f,
		  CreateQueueF _create_queue_f) :
    priority_queue(_create_queue_f(std::bind(&SimulatedServer::has_avail_thread,
					     this),
				   std::bind(&SimulatedServer::inner_post,
					     this,
					     std::placeholders::_1,
					     std::placeholders::_2,
					     std::placeholders::_3,
					     std::placeholders::_4))),
    client_resp_f(_client_resp_f),
    thread_pool_size(_thread_pool_size),
    finishing(false)
  {
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

  void post(TestRequest&& request,
	    const ClientId& client_id,
	    const ReqPm& req_params,
	    const Cost request_cost)
  {
    priority_queue->add_request(std::move(request),
				client_id,
				req_params,
				request_cost);
  }

  bool has_avail_thread() {
    InnerQGuard g(inner_queue_mtx);
    return inner_queue.size() <= thread_pool_size;
  }

  const Q& get_priority_queue() const { return *priority_queue; }

protected:

  void inner_post(const ClientId& client,
		  std::unique_ptr<TestRequest> request,
		  const RespPm& additional,
		  const Cost request_cost) {
    Lock l(inner_queue_mtx);
    assert(!finishing);
    inner_queue.emplace_back(QueueItem(client,
				       std::move(request),
				       additional,
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
	auto client = front.client;
	auto req = std::move(front.request);
	auto additional = front.additional;
	auto request_cost = front.request_cost;
	inner_queue.pop_front();

	l.unlock();

	// TODO: rather than assuming this constructor exists, perhaps
	// pass in a function that does this mapping?
	client_resp_f(client, TestResponse{req->epoch}, additional, request_cost);

	priority_queue->request_completed();

	l.lock(); // in prep for next iteration of loop
      } else {
	break;
      }
    }
  }
}; // class SimulatedServer

