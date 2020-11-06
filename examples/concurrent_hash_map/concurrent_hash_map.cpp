// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2019-2020, Intel Corporation */
/*
 * concurrent_hash_map.cpp -- C++ documentation snippets.
 */
//! [concurrent_hash_map_ex]
#include <iostream>
#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <vector>
#include <chrono>
#include <mutex>
#include <condition_variable>
//dennis, allocate the key/value from the memkind
//#include "pmem_allocator.h"
//#include <scoped_allocator>
//#include <string>
//#include <tbb/concurrent_hash_map.h>
using namespace pmem::obj;
/* In this example we will be using concurrent_hash_map with p<int> type for
 * both keys and values */
using hashmap_type = concurrent_hash_map<p<int>, p<int>>;
const int THREADS_NUM = 10;
// !!!!!!!!!!!!!!!
bool is_write=true; 
/* This is basic example and we only need to use concurrent_hash_map. Hence we
 * will correlate memory pool root object with single instance of persistent
 * pointer to hasmap_type */
struct root {
	persistent_ptr<hashmap_type> pptr;
};

std::vector<std::chrono::steady_clock::time_point> times_start;
std::vector<std::chrono::steady_clock::time_point> times_end;

template <typename Function>
void
parallel_exec(size_t concurrency, Function f)
{
	std::vector<std::thread> threads;
	threads.reserve(concurrency);

	for (size_t i = 0; i < concurrency; ++i) {
		threads.emplace_back(f, i);
	}

	for (auto &t : threads) {
		t.join();
	}
}

/*
 * This function executes 'concurrency' threads and provides
 * 'syncthreads' method (synchronization barrier) for f()
 */
template <typename Function>
void
parallel_xexec(size_t concurrency, Function f)
{
	std::condition_variable cv;
	std::mutex m;
	std::unique_ptr<size_t> counter =
		std::unique_ptr<size_t>(new size_t(0));

	auto syncthreads = [&] {
		std::unique_lock<std::mutex> lock(m);
		(*counter)++;
		if (*counter < concurrency)
			cv.wait(lock, [&] { return *counter >= concurrency; });
		else
			/*
			 * notify_call could be called outside of a lock
			 * (it would perform better) but drd complains
			 * in that case
			 */
			cv.notify_all();
	};

	parallel_exec(concurrency, [&](size_t tid) { f(tid, syncthreads); });
}



/* Before running this example, run:
 * pmempool create obj --layout="concurrent_hash_map" --size 1G path_to_a_pool
 */
int
main(int argc, char *argv[])
{
	pool<root> pop;
		try {
			pop = pool<root>::open("/dev/shm/c1", "concurrent_hash_map");
		} catch (pmem::pool_error &e) {
			std::cerr << e.what() << std::endl;
			return -1;
		}
		auto &r = pop.root()->pptr;
		if (r == nullptr) {
			printf("the root is not allocated..\n");
			pmem::obj::transaction::run(pop, [&] {
				r = make_persistent<hashmap_type>();
			});
			r->runtime_initialize();
		} else {
			/* Logic when hash_map already exists. After opening of
			 * the pool we have to call runtime_initialize()
			 * function in order to recalculate mask and check for
			 * consistentcy. */
			printf("the root is allocated\n");
			r->runtime_initialize();
		}
		auto &map = *r;
		std::vector<std::thread> threads;
		threads.reserve(static_cast<size_t>(THREADS_NUM));
		long WRITE_SIZE  = 1e6;
		long READ_SIZE = WRITE_SIZE;
		clock_t start, end;
		int k=map.size();
		//dennis check the keys restores or not??
		//printf("the items account=%d\n", std::distance(map.begin(), map.end()));
		if(k!=0) is_write=false;
		else is_write=true;		
		// write 
		/* Insert WRITE_SIZE key-value pairs to the hashmap. This
		 * operation is thread-safe. */
		if (is_write)
		{
			start = clock();
			for (int thr = 0; thr < THREADS_NUM; thr++) {	
				int t = thr;
				threads.emplace_back([&, t]() {
					for (int i = t*WRITE_SIZE/THREADS_NUM; i < (t+1)*WRITE_SIZE/THREADS_NUM; i++) {
						map.insert(
							hashmap_type::value_type(i, i));
					}
				});
			}
			for (auto &t : threads) {
				t.join();
			}
			end = clock();
			printf("write avg time = %lf us \n", 
					double(end - start) * 1e6 / CLOCKS_PER_SEC / WRITE_SIZE);  // 1M us = 1s
			threads.clear();
		}
		std::cout << "items count " <<  map.size() << std::endl;

		times_start.resize(THREADS_NUM);
		times_end.resize(THREADS_NUM);

		parallel_xexec(THREADS_NUM, [&](size_t thread_id, std::function<void(void)> syncthreads){
			syncthreads();
			times_start[thread_id] = std::chrono::steady_clock::now();
			for (size_t i = thread_id*READ_SIZE/THREADS_NUM; i < (thread_id+1)*READ_SIZE/THREADS_NUM; i++) {
				hashmap_type::accessor acc;
				bool res = map.find(acc, i);
				if (res) {
				}
			}
			times_end[thread_id] = std::chrono::steady_clock::now();
			syncthreads();
		});

		auto min = std::min_element(times_start.begin(), times_start.end());
		auto max = std::max_element(times_end.begin(), times_end.end());
		
		std::cout << "duration " << std::chrono::duration_cast<std::chrono::milliseconds>(
			*max - *min).count() << std::endl;

		size_t sum = 0;

		for (int i = 0; i < THREADS_NUM; i++) {
			auto secs = std::chrono::duration_cast<std::chrono::microseconds>(
			times_end[i] - times_start[i]).count();

			sum += secs;
		}

		std::cout << "sum " << sum << std::endl;

		pop.close();

	return 0;
}
//! [concurrent_hash_map_ex]