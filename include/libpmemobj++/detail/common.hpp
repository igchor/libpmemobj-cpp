/*
 * Copyright 2016-2018, Intel Corporation
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
 * Commonly used functionality.
 */

#ifndef LIBPMEMOBJ_CPP_COMMON_HPP
#define LIBPMEMOBJ_CPP_COMMON_HPP

#include "libpmemobj++/detail/pexceptions.hpp"
#include "libpmemobj++/policy.hpp"
#include "libpmemobj/tx_base.h"

#include <typeinfo>

#if defined(__GNUC__) || defined(__clang__)
#define POBJ_CPP_DEPRECATED __attribute__((deprecated))
#elif defined(_MSC_VER)
#define POBJ_CPP_DEPRECATED __declspec(deprecated)
#else
#define POBJ_CPP_DEPRECATED
#endif

namespace pmem
{

namespace obj
{
template <typename T>
class persistent_ptr;
}

namespace detail
{

template <typename T, pmem::obj::Policy policy>
inline typename std::enable_if<policy == pmem::obj::Policy::weak>::type
check_policy(const T *that)
{
	return;
}

template <typename T, pmem::obj::Policy policy>
inline typename std::enable_if<policy == pmem::obj::Policy::tx_only>::type
check_policy(const T *that)
{
	if (pmemobj_tx_stage() != TX_STAGE_WORK && pmemobj_pool_by_ptr(that))
		throw transaction_error(
			"Pmem object modified outside of a transaction");
}

template <typename T, pmem::obj::Policy policy>
inline typename std::enable_if<policy == pmem::obj::Policy::pmem_only>::type
check_policy(const T *that)
{
	if (!pmemobj_pool_by_ptr(that))
		throw transaction_error("Object is not on pmem");
}

template <typename T, pmem::obj::Policy policy>
inline typename std::enable_if<policy == pmem::obj::Policy::restricted>::type
check_policy(const T *that)
{
	check_policy<T, pmem::obj::Policy::pmem_only>(that);
	check_policy<T, pmem::obj::Policy::tx_only>(that);
}
/*
 * Add count objects starting from `that` to the transaction. Policy
 * template parameter specifies behaviour od this function when it is called
 * with pointer to element on stack or is called outside transaction.
 *
 * @param[in] that pointer to the first object being added to the transaction.
 * @param[in] count number of elements to be added to the transaction.
 */
template <typename T, pmem::obj::Policy policy>
inline void
add_to_tx(const T *that, std::size_t count = 1)
{
	check_policy<T, policy>(that);

	if (count == 0)
		return;

	if (pmemobj_tx_stage() != TX_STAGE_WORK)
		return;

	/* 'that' is not in any open pool */
	if (!pmemobj_pool_by_ptr(that))
		return;

	if (pmemobj_tx_add_range_direct(that, sizeof(*that) * count))
		throw transaction_error("Could not add object(s) to the"
					" transaction.");
}

template <typename T>
inline void
conditional_add_to_tx(const T *that, std::size_t count = 1)
{
	return add_to_tx<T, pmem::obj::Policy::weak>(that, count);
}

/*
 * Return type number for given type.
 */
template <typename T>
uint64_t
type_num()
{
	return typeid(T).hash_code();
}

/**
 * Round up to the next lowest power of 2. Overload for uint64_t argument.
 */
inline uint64_t
next_pow_2(uint64_t v)
{
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v |= v >> 32;
	++v;
	return v + (v == 0);
}

/**
 * Round up to the next lowest power of 2. Overload for uint32_t argument.
 */
inline uint64_t
next_pow_2(uint32_t v)
{
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	++v;
	return v + (v == 0);
}

} /* namespace detail */

} /* namespace pmem */

#endif /* LIBPMEMOBJ_CPP_COMMON_HPP */
