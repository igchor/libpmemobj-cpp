// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

/**
 * @file
 * Implementation of redo log based API.
 */

#ifndef LIBPMEMOBJ_CPP_ACTIONS_HPP
#define LIBPMEMOBJ_CPP_ACTIONS_HPP

#include <libpmemobj.h>

#include <libpmemobj++/detail/life.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

#include <unordered_map>
#include <vector>

struct pmemobjpool {
	char padd[4096];
	//struct pool_hdr hdr;	/* memory pool header */

	/* persistent part of PMEMOBJ pool descriptor (2kB) */
	char layout[((size_t)1024)];
	uint64_t lanes_offset;
	uint64_t nlanes;
	uint64_t heap_offset;
	uint64_t unused3;
	unsigned char unused[2048 - 40 - 1024]; /* must be zero */
	uint64_t checksum;	/* checksum of above fields */

	uint64_t root_offset;

	/* unique runID for this program run - persistent but not checksummed */
	uint64_t run_id;

	uint64_t root_size;

	/*
	 * These flags can be set from a conversion tool and are set only for
	 * the first recovery of the pool.
	 */
	uint64_t conversion_flags;

	uint64_t heap_size;
};

#define PTR_FROM_POOL(pop, ptr)\
	((uintptr_t)(ptr) >= (uintptr_t)(pop) &&\
	(uintptr_t)(ptr) < (uintptr_t)(pop) +\
	(pop)->heap_offset + (pop)->heap_size)

namespace pmem
{

namespace obj
{

template <typename T>
struct persistent_ptr;

namespace experimental
{

struct actions {

	actions(pool_base pop, std::size_t cap = 4) : pop(pop.handle())
	{
		acts.reserve(cap);
	}

	template <typename T>
	void
	set(T *w, T value)
	{
		if (!PTR_FROM_POOL(((pmemobjpool*) pop), w)) {
			*w = value;
			return;
		}

		auto it = wal.find((uint64_t *)w);
		if (it != wal.end())
			it->second = (uint64_t)value;
		else
			wal.emplace((uint64_t *)w, value);
	}

	template <typename T>
	T
	get(T *addr)
	{
		auto it = wal.find((uint64_t *)addr);
		if (it != wal.end())
			return (T)it->second;

		return *((T *)addr);
	}

	template <typename T>
	void
	free(persistent_ptr<T> ptr)
	{
		acts.emplace_back();
		pmemobj_defer_free(pop, ptr.raw(), &acts.back());
	}

	void *
	allocate(uint64_t size)
	{
		acts.emplace_back();
		auto ptr = pmemobj_reserve(pop, &acts.back(), size, 0);

		return pmemobj_direct(ptr);
	}

	void
	publish()
	{
		if (acts.size() == 0 && wal.size() == 0)
			return;

		acts.reserve(acts.size() + wal.size());
		for (auto &v : wal) {
			acts.emplace_back();
			pmemobj_set_value(pop, &acts.back(), v.first, v.second);
		}

		if (pmemobj_publish(pop, acts.data(), acts.size()))
			throw std::runtime_error("XXX");
	}

	void
	cancel()
	{
		if (acts.size() == 0 && wal.size() == 0)
			return;

		pmemobj_cancel(pop, acts.data(), acts.size());
	}

private:
	std::vector<pobj_action> acts;
	std::unordered_map<uint64_t *, uint64_t> wal;
	PMEMobjpool *pop;
};

struct actions_tx {
	static actions *
	get_state()
	{
		return state();
	}

	template <typename F>
	static void
	run(pool_base pop, F &&f)
	{
		auto init_s = state();
		if (!init_s)
			state() = new actions(pop);

		try {
			f();

			if (!init_s)
				state()->publish();
		} catch (...) {
			if (!init_s) {
				state()->cancel();
				delete state();
			}

			state() = init_s;
			throw;
		}

		if (!init_s)
			delete state();

		state() = init_s;
	}

private:
	static actions *&
	state()
	{
		thread_local actions *a = nullptr;
		return a;
	}
};

template <typename T>
struct actions_allocator {
	template <class U>
	struct rebind {
		using other = actions_allocator<U>;
	};

	persistent_ptr<T>
	allocate(size_t cnt = 1)
	{
		auto state = actions_tx::get_state();
		return persistent_ptr<T>((T *)state->allocate(cnt * sizeof(T)));
	}

	void
	deallocate(persistent_ptr<T> ptr)
	{
		auto state = actions_tx::get_state();
		state->free(ptr);
	}

	template <typename... Args>
	void
	construct(persistent_ptr<T> ptr, Args &&... args)
	{
		new (ptr.get()) T(std::forward<Args>(args)...);
	}

	void
	destroy(persistent_ptr<T> ptr)
	{
		detail::destroy<T>(*ptr.get());
	}
};

template <>
struct actions_allocator<void> {

	persistent_ptr<void>
	allocate(size_t cnt)
	{
		auto state = actions_tx::get_state();
		return state->allocate(cnt);
	}
};

template <typename T>
struct r {
	r()
	{
	}

	r(const T &rhs) : val(rhs)
	{
	}

	r(const r &rhs)
	{
		auto state = actions_tx::get_state();

		if (!state)
			val = rhs.val;
		else
			val = state->get<T>(&rhs.val);
	}

	r &
	operator=(const T &rhs)
	{
		auto state = actions_tx::get_state();
		if (!state)
			val = rhs;
		else
			state->set(&val, rhs);

		return *this;
	}

	r &
	operator=(const r &rhs)
	{
		auto state = actions_tx::get_state();
		if (!state)
			val = rhs.val;
		else
			state->set(&val, state->get<T>(&rhs.val));

		return *this;
	}

	operator T() const
	{
		auto state = actions_tx::get_state();

		if (!state)
			return val;

		return state->get(&val);
	}

	r &
	operator++()
	{
		auto state = actions_tx::get_state();
		if (!state)
			val++;
		else
			state->set(&val, state->get<T>(&val) + 1);

		return *this;
	}

	r &
	operator--()
	{
		auto state = actions_tx::get_state();
		if (!state)
			val--;
		else
			state->set(&val, state->get<T>(&val) - 1);

		return *this;
	}

	void
	operator|=(const T &rhs)
	{
		auto state = actions_tx::get_state();
		if (!state)
			val |= rhs;
		else
			state->set(&val, state->get<T>(&val) | rhs);
	}

	void
	operator|=(const r &rhs)
	{
		auto state = actions_tx::get_state();
		if (!state)
			val |= rhs.val;
		else
			state->set(&val,
				   state->get<T>(&val) |
					   state->get<T>(&rhs.val));
	}

private:
	T val;
};

}
}
}

#endif
