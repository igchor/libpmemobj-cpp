/*
 * Copyright 2019, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @file
 * volatile state for persistent objects.
 *
 * This feature requires C++14 support.
 */

#ifndef LIBPMEMOBJ_CPP_VOLATILE_STATE_HPP
#define LIBPMEMOBJ_CPP_VOLATILE_STATE_HPP

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <tuple>
#include <unordered_map>

#include <libpmemobj++/detail/common.hpp>
#include <libpmemobj++/detail/life.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

namespace pmem
{

namespace detail
{

class volatile_state {
public:
	volatile_state() = default;

	/*
	 * We cannot use those ctors inside of a transaction so do not implement
	 * them for now.
	 *
	 * Moreover, for move ctor we would have to copy the pointer to another
	 * entry in hash_table (corresponding to the oid of object we are
	 * constructing) and clear the old pointer on tx commit. The problem is
	 * if the original object would be destroyed - the pointer would be
	 * freed. To solve that we could use shared_ptr.
	 */
	volatile_state(const volatile_state &) = delete;

	volatile_state &operator=(const volatile_state &) = delete;

	template <typename T>
	T *
	get()
	{
		return get<T>(pmemobj_oid(this));
	}

	template <typename T>
	T *
	get_if_exists()
	{
		return get_if_exists<T>(pmemobj_oid(this));
	}

	~volatile_state()
	{
		destroy(pmemobj_oid(this));
	}

	template <typename T>
	static T *
	get_if_exists(const PMEMoid &oid)
	{
		auto &map = get_map();

		{
			std::shared_lock<rwlock_type> lock(get_rwlock());
			auto it = map.find(oid);
			if (it != map.end())
				return static_cast<T *>(it->second.get());
			else
				return nullptr;
		}
	}

	template <typename T>
	static T *
	get(const PMEMoid &oid)
	{
		auto &map = get_map();

		auto element = get_if_exists<T>(oid);
		if (element)
			return element;

		if (pmemobj_tx_stage() == TX_STAGE_WORK)
			throw pmem::transaction_scope_error(
				"get() cannot be called in a transaction");

		{
			std::unique_lock<rwlock_type> lock(get_rwlock());

			auto deleter = [](void const *data) {
				T const *p = static_cast<T const *>(data);
				delete p;
			};

			auto it = map.emplace(std::piecewise_construct,
					      std::forward_as_tuple(oid),
					      std::forward_as_tuple(new T,
								    deleter))
					  .first;

			/*
			 * emplace() could failed if another thread created
			 * the element when we dropped read and acquired write
			 * lock, in that case it will just point to existing
			 * element.
			 */
			return static_cast<T *>(it->second.get());
		}
	}

	static void
	destroy(const PMEMoid &oid)
	{
		if (pmemobj_tx_stage() == TX_STAGE_WORK) {
			obj::transaction::register_callback(
				obj::transaction::stage::on_commit{}, [oid] {
					std::unique_lock<rwlock_type> lock(
						get_rwlock());
					get_map().erase(oid);
				});
		} else { /* XXX: only if TX_STAGE_NONE and error otherwise? */
			std::unique_lock<rwlock_type> lock(get_rwlock());
			get_map().erase(oid);
		}
	}

	static void
	clear_from_pool(uint64_t pool_id)
	{
		std::unique_lock<rwlock_type> lock(get_rwlock());
		auto &map = get_map();

		for (auto it = map.begin(); it != map.end();) {
			if (it->first.pool_uuid_lo == pool_id)
				it = map.erase(it);
			else
				++it;
		}
	}

private:
	struct pmemoid_hash {
		std::size_t
		operator()(const PMEMoid &oid) const
		{
			return oid.pool_uuid_lo + oid.off;
		}
	};

	struct pmemoid_equal_to {
		bool
		operator()(const PMEMoid &lhs, const PMEMoid &rhs) const
		{
			return lhs.pool_uuid_lo == rhs.pool_uuid_lo &&
				lhs.off == rhs.off;
		}
	};

	using map_type = std::unordered_map<
		PMEMoid,
		std::unique_ptr<void,
				std::add_pointer<void(const void *)>::type>,
		pmemoid_hash, pmemoid_equal_to>;

	using rwlock_type = std::shared_timed_mutex;

	struct register_on_pool_close_callback {
		register_on_pool_close_callback()
		{
			pmem::obj::pool_base::register_on_close_callback(
				volatile_state::clear_from_pool);
		}
	};

	static map_type &
	get_map()
	{
		/* This will register callback to be called by pool.close()
		 * when get_map() is first called */
		static register_on_pool_close_callback _;

		static map_type map;

		return map;
	}

	static rwlock_type &
	get_rwlock()
	{
		static rwlock_type rwlock;
		return rwlock;
	}
};

} /* namespace detail */

} /* namespace pmem */

#endif /* LIBPMEMOBJ_CPP_VOLATILE_STATE_HPP */
