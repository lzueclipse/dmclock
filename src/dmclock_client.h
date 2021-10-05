// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2017 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#pragma once

#include <map>
#include <deque>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "../support/src/run_every.h"
#include "dmclock_util.h"
#include "dmclock_recs.h"


namespace crimson {
  namespace dmclock {

    // OrigTracker is a best-effort implementation of the the original
    // dmClock calculations of delta, rho and lambda. It adheres to an
    // interface, implemented via a template type, that allows it to
    // be replaced with an alternative. The interface consists of the
    // static create, prepare_req, resp_update, and get_last_delta
    // functions.
    class OrigTracker {
      Counter   delta_prev_req;
      Counter   rho_prev_req;
      Counter   lambda_prev_req;
      uint32_t  my_delta;
      uint32_t  my_rho;
      uint64_t  my_lambda;

    public:

      OrigTracker(Counter global_delta,
		  Counter global_rho,
		  Counter global_lambda) :
	delta_prev_req(global_delta),
	rho_prev_req(global_rho),
	lambda_prev_req(global_lambda),
	my_delta(0),
	my_rho(0),
	my_lambda(0)
      { /* empty */ }

      static inline OrigTracker create(Counter the_delta, Counter the_rho, Counter the_lambda) {
	return OrigTracker(the_delta, the_rho, the_lambda);
      }

      inline ReqParams prepare_req(Counter& the_delta, Counter& the_rho, Counter& the_lambda) {
	Counter delta_out = the_delta - delta_prev_req - my_delta;
	Counter rho_out = the_rho - rho_prev_req - my_rho;
	Counter lambda_out = the_lambda - lambda_prev_req - my_lambda;
	delta_prev_req = the_delta;
	rho_prev_req = the_rho;
	lambda_prev_req = the_lambda;
	my_delta = 0;
	my_rho = 0;
	my_lambda = 0;
	return ReqParams(uint32_t(delta_out), uint32_t(rho_out), uint64_t(lambda_out));
      }

      inline void resp_update(PhaseType phase,
			      Counter& the_delta,
			      Counter& the_rho,
			      Cost cost,
			      Counter& the_lambda,
			      Counter length) {
	the_delta += cost;
	my_delta += cost;
	if (phase == PhaseType::reservation) {
	  the_rho += cost;
	  my_rho += cost;
	}
	the_lambda += length;
	my_lambda += length;
      }

      inline Counter get_last_delta() const {
	return delta_prev_req;
      }
    }; // struct OrigTracker


    /*
     * S is server identifier type
     *
     * T is the server info class that adheres to ServerTrackerIfc
     * interface
     */
    template<typename S, typename T = OrigTracker>
    class ServiceTracker {
      // we don't want to include gtest.h just for FRIEND_TEST
      friend class dmclock_client_server_erase_Test;

      using TimePoint = decltype(std::chrono::steady_clock::now());
      using Duration = std::chrono::milliseconds;
      using MarkPoint = std::pair<TimePoint,Counter>;

      Counter                 delta_counter; // # reqs completed
      Counter                 rho_counter;   // # reqs completed via reservation
      Counter                 lambda_counter; // # reqs completed length (in bytes)
      std::map<S,T>           server_map;
      mutable std::mutex      data_mtx;      // protects Counters and map

      using DataGuard = std::lock_guard<decltype(data_mtx)>;

      // clean config

      std::deque<MarkPoint>     clean_mark_points;
      Duration                  clean_age;     // age at which server tracker cleaned

      // NB: All threads declared at end, so they're destructed firs!

      std::unique_ptr<RunEvery> cleaning_job;


    public:

      // we have to start the counters at 1, as 0 is used in the
      // cleaning process
      template<typename Rep, typename Per>
      ServiceTracker(std::chrono::duration<Rep,Per> _clean_every,
		     std::chrono::duration<Rep,Per> _clean_age) :
	delta_counter(1),
	rho_counter(1),
	lambda_counter(1),
	clean_age(std::chrono::duration_cast<Duration>(_clean_age))
      {
	cleaning_job =
	  std::unique_ptr<RunEvery>(
	    new RunEvery(_clean_every,
			 std::bind(&ServiceTracker::do_clean, this)));
      }


      // the reason we're overloading the constructor rather than
      // using default values for the arguments is so that callers
      // have to either use all defaults or specify all timings; with
      // default arguments they could specify some without others
      ServiceTracker() :
	ServiceTracker(std::chrono::minutes(5), std::chrono::minutes(10))
      {
	// empty
      }


      /*
       * Incorporates the response data received into the counters.
       */
      void track_resp(const S& server_id,
		      const PhaseType& phase,
		      Counter request_cost = 1u,
		      Counter request_length = 0u) {
	DataGuard g(data_mtx);

	auto it = server_map.find(server_id);
	if (server_map.end() == it) {
	  // this code can only run if a request did not precede the
	  // response or if the record was cleaned up b/w when
	  // the request was made and now
	  auto i = server_map.emplace(server_id,
				      T::create(delta_counter, rho_counter, lambda_counter));
	  it = i.first;
	}
	it->second.resp_update(phase, delta_counter, rho_counter, request_cost, lambda_counter, request_length);
      }

      /*
       * Returns the ReqParams for the given server.
       */
      ReqParams get_req_params(const S& server) {
	DataGuard g(data_mtx);
	auto it = server_map.find(server);
	if (server_map.end() == it) {
	  server_map.emplace(server,
			     T::create(delta_counter, rho_counter, lambda_counter));
	  return ReqParams(1, 1, 1);
	} else {
	  return it->second.prepare_req(delta_counter, rho_counter, lambda_counter);
	}
      }

    private:

      /*
       * This is being called regularly by RunEvery. Every time it's
       * called it notes the time and delta counter (mark point) in a
       * deque. It also looks at the deque to find the most recent
       * mark point that is older than clean_age. It then walks the
       * map and delete all server entries that were last used before
       * that mark point.
       */
      void do_clean() {
	TimePoint now = std::chrono::steady_clock::now();
	DataGuard g(data_mtx);
	clean_mark_points.emplace_back(MarkPoint(now, delta_counter));

	Counter earliest = 0;
	auto point = clean_mark_points.front();
	while (point.first <= now - clean_age) {
	  earliest = point.second;
	  clean_mark_points.pop_front();
	  point = clean_mark_points.front();
	}

	if (earliest > 0) {
	  for (auto i = server_map.begin();
	       i != server_map.end();
	       /* empty */) {
	    auto i2 = i++;
	    if (i2->second.get_last_delta() <= earliest) {
	      server_map.erase(i2);
	    }
	  }
	}
      } // do_clean
    }; // class ServiceTracker
  }
}
