// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

/**
 * @file
 * Implementation of redo log based API.
 */

#ifndef LIBPMEMOBJ_CPP_ACTIONS_HPP
#define LIBPMEMOBJ_CPP_ACTIONS_HPP

#include <libpmemobj.h>

#include <unordered_map>
#include <vector>

namespace pmem
{

namespace obj
{

template <typename T>
struct persistent_ptr;

namespace experimental
{

struct actions {

	actions(PMEMobjpool *pop, std::size_t cap = 4)
	    : pop(pop)
	{
		acts.reserve(cap);
	}

	template <typename T>
	void
	set(T *w, T value)
	{
		auto it = wal.find((uint64_t*)w);
		if (it != wal.end())
			it->second = (uint64_t) value;
		else
			wal.emplace((uint64_t*)w, value);
	}

	template <typename T>
	T get(T *addr)
	{
		auto it = wal.find((uint64_t*)addr);
		if (it != wal.end())
			return (T) it->second;

		return *((T*)addr);
	}

	template <typename T>
	void
	free(persistent_ptr<T> ptr)
	{
		acts.emplace_back();
		pmemobj_defer_free(pop, ptr.raw(),
				   &acts.back());
	}

	template <typename T, typename... Args>
	obj::persistent_ptr<T>
	make(uint64_t size, Args &&... args)
	{
		acts.emplace_back();
		obj::persistent_ptr<T> ptr =
			pmemobj_reserve(pop, &acts.back(), size, 0);

		new (ptr.get()) T(std::forward<Args>(args)...);

		return ptr;
	}

	void
	publish()
	{
		acts.reserve(acts.size() + wal.size());
		for (auto &v : wal) {
			acts.emplace_back();
			pmemobj_set_value(pop, &acts.back(), v.first, v.second);
		}

		if (pmemobj_publish(pop, acts.data(), acts.size()))
			throw std::runtime_error("XXX");
	}

	void cancel() {
		pmemobj_cancel(pop, acts.data(), acts.size());
	}

private:
	std::vector<pobj_action> acts;
	std::unordered_map<uint64_t*, uint64_t> wal;
	PMEMobjpool* pop;
};

struct actions_tx {
	static actions* get_state()
    {
		return state();
    }

	template <typename F, typename... Args>
    static void run(PMEMobjpool* pop, F&& f, Args&&... args)
    {
		auto acts = std::unique_ptr<actions>(new actions(pop));
		state() = acts.get();

		try {
        	f(std::forward<Args>(args)...);

			acts->publish();
		} catch (...) {
			acts->cancel();
			state() = nullptr;
			throw;
		}

		state() = nullptr;
    }

private:
	static actions*& state()
	{
		thread_local actions* a = nullptr;
		return a;
	}
};

}
}
}

#endif