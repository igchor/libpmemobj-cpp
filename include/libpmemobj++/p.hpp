/*
 * Copyright 2015-2018, Intel Corporation
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
 * Resides on pmem property template.
 */

#ifndef LIBPMEMOBJ_CPP_P_HPP
#define LIBPMEMOBJ_CPP_P_HPP

#include "libpmemobj++/detail/common.hpp"
#include "libpmemobj++/detail/specialization.hpp"
#include "libpmemobj++/policy.hpp"

#include <memory>

namespace pmem
{

namespace obj
{
/**
 * Resides on pmem class.
 *
 * p class is a property-like template class that has to be used for all
 * variables (excluding persistent pointers), which are used in pmemobj
 * transactions. The p property makes sure that changes to a variable within
 * a transaction are made atomically with respect to persistence. It does it by
 * creating a snapshot of the variable when modified in the transaction scope.
 * The p class is not designed to be used with compound types. For that see the
 * persistent_ptr.
 * @snippet doc_snippets/persistent.cpp p_property_example
 */
template <typename T, Policy P = Policy::weak>
class p {
	typedef p<T, P> this_type;

public:
	static constexpr Policy policy = P;

	/**
	 * Value constructor.
	 *
	 * Directly assigns a value to the underlying storage.
	 *
	 * @param _val const reference to the value to be assigned.
	 */
	p(const T &_val)
	{
		detail::check_policy<p, policy>(this);
	}

	p(const p &rhs)
	{
		detail::check_policy<p, policy>(this);
	}

	/**
	 * Defaulted constructor.
	 */
	p()
	{
		detail::check_policy<p, policy>(this);
	}

	/**
	 * Assignment operator.
	 *
	 * The p<> class property assignment within a transaction
	 * automatically registers this operation so that a rollback
	 * is possible.
	 *
	 * @throw pmem::transaction_error when adding the object to the
	 *	transaction failed.
	 */
	p &
	operator=(const p &rhs)
	{
		get_rw() = rhs.get_ro();

		return *this;
	}

	p &
	operator=(const T &rhs)
	{
		get_rw() = rhs;

		return *this;
	}

	/**
	 * Converting assignment operator from a different p<>.
	 *
	 * Available only for convertible types.
	 * Just like regular assignment, also automatically registers
	 * itself in a transaction.
	 *
	 * @throw pmem::transaction_error when adding the object to the
	 *	transaction failed.
	 */
	template <typename Y, Policy U,
		  typename = typename std::enable_if<
			  std::is_convertible<Y, T>::value>::type>
	p &
	operator=(const p<Y, U> &rhs)
	{
		get_rw() = rhs.get_ro();

		return *this;
	}

	/**
	 * Conversion operator back to the underlying type.
	 */
	operator T() const noexcept
	{
		return this->val;
	}

	/**
	 * Retrieves read-write reference of the object.
	 *
	 * The entire object is automatically added to the transaction.
	 *
	 * @return a reference to the object.
	 *
	 * @throw pmem::transaction_error when adding the object to the
	 *	transaction failed.
	 */
	T &
	get_rw()
	{
		detail::add_to_tx<p, policy>(this);

		return this->val;
	}

	/**
	 * Retrieves read-only const reference of the object.
	 *
	 * This method has no transaction side effects.
	 *
	 * @return a const reference to the object.
	 */
	const T &
	get_ro() const noexcept
	{
		return this->val;
	}

	/**
	 * Swaps two p objects of the same type.
	 *
	 * @throw pmem::transaction_error when adding the object to the
	 *	transaction failed.
	 */
	template <Policy U>
	void
	swap(p<T, U> &other)
	{
		std::swap(get_rw(), other.get_rw());
	}

private:
	T val;
};

/**
 * Swaps two p objects of the same type.
 *
 * Non-member swap function as required by Swappable concept.
 * en.cppreference.com/w/cpp/concept/Swappable
 */
template <class T, Policy P, Policy U>
inline void
swap(p<T, P> &a, p<T, U> &b)
{
	a.swap(b);
}

namespace weak
{
template <typename T>
using p = ::pmem::obj::p<T, ::pmem::obj::Policy::weak>;
}

namespace tx_only
{
template <typename T>
using p = ::pmem::obj::p<T, ::pmem::obj::Policy::tx_only>;
}

namespace pmem_only
{
template <typename T>
using p = ::pmem::obj::p<T, ::pmem::obj::Policy::pmem_only>;
}

namespace restricted
{
template <typename T>
using p = ::pmem::obj::p<T, ::pmem::obj::Policy::restricted>;
}

} /* namespace obj */

} /* namespace pmem */

#endif /* LIBPMEMOBJ_CPP_P_HPP */
