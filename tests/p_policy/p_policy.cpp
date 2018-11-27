/*
 * Copyright 2018, Intel Corporation
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

#include "unittest.hpp"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#define LAYOUT "cpp"

namespace nvobj = pmem::obj;

namespace
{

template <nvobj::Policy policy>
struct foo {
	template <typename T>
	using p = nvobj::p<T, policy>;

	p<int> pint = 0;
	p<char> pchar = 0;
};

struct root {
	nvobj::persistent_ptr<foo<nvobj::Policy::weak>> ptr_weak;
	nvobj::persistent_ptr<foo<nvobj::Policy::pmem_only>> ptr_pmem;
	nvobj::persistent_ptr<foo<nvobj::Policy::tx_only>> ptr_tx;
	nvobj::persistent_ptr<foo<nvobj::Policy::restricted>> ptr_restricted;
};

void
test_policy_weak(nvobj::pool<root> &pop)
{
	auto r = pop.root();

	try {
		nvobj::transaction::run(pop, [&] {
			r->ptr_weak = nvobj::make_persistent<
				foo<nvobj::Policy::weak>>();
		});
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification outside of a transaction allowed */
		r->ptr_weak->pint = 10;
		r->ptr_weak->pint = r->ptr_weak->pchar;
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification on stack allowed */
		nvobj::p<int, nvobj::Policy::weak> pint;
		pint = 10;

		nvobj::p<int, nvobj::Policy::weak> pint2(r->ptr_weak->pchar);
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification inside of a transaction on pmem always allowed
		 */
		nvobj::transaction::run(pop, [&] {
			r->ptr_weak->pint = 4;
			r->ptr_weak->pint = r->ptr_weak->pchar;

			r->ptr_weak->pint += r->ptr_weak->pchar;
			r->ptr_weak->pint += char(10);
			r->ptr_weak->pint += int(10);

			nvobj::delete_persistent<foo<nvobj::Policy::weak>>(
				r->ptr_weak);
		});
	} catch (...) {
		UT_ASSERT(0);
	}

	static_assert(
		std::is_same<pmem::obj::weak::p<int>,
			     pmem::obj::p<int, pmem::obj::Policy::weak>>::value,
		"");
}

void
test_policy_restricted(nvobj::pool<root> &pop)
{
	auto r = pop.root();
	try {
		nvobj::transaction::run(pop, [&] {
			r->ptr_restricted = nvobj::make_persistent<
				foo<nvobj::Policy::restricted>>();
		});
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification outside of a transaction NOT allowed */
		r->ptr_restricted->pint = 10;

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification outside of a transaction NOT allowed */
		r->ptr_restricted->pint = r->ptr_restricted->pchar;

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification on stack NOT allowed */
		nvobj::p<int, nvobj::Policy::restricted> pint;

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification on stack NOT allowed */
		nvobj::p<int, nvobj::Policy::restricted> pint(
			r->ptr_restricted->pchar);

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification inside of a transaction on pmem always allowed
		 */
		nvobj::transaction::run(pop, [&] {
			r->ptr_restricted->pint = 4;
			r->ptr_restricted->pint = r->ptr_restricted->pchar;

			r->ptr_restricted->pint += r->ptr_restricted->pchar;
			r->ptr_restricted->pint += char(10);
			r->ptr_restricted->pint += int(10);

			nvobj::delete_persistent<
				foo<nvobj::Policy::restricted>>(
				r->ptr_restricted);
		});
	} catch (...) {
		UT_ASSERT(0);
	}

	static_assert(
		std::is_same<pmem::obj::restricted::p<int>,
			     pmem::obj::p<int, pmem::obj::Policy::restricted>>::
			value,
		"");
}

void
test_policy_pmem(nvobj::pool<root> &pop)
{
	auto r = pop.root();

	try {
		nvobj::transaction::run(pop, [&] {
			r->ptr_pmem = nvobj::make_persistent<
				foo<nvobj::Policy::pmem_only>>();
		});
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification outside of a transaction allowed */
		r->ptr_pmem->pint = 10;
		r->ptr_pmem->pint = r->ptr_pmem->pchar;
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification on stack NOT allowed */
		nvobj::p<int, nvobj::Policy::pmem_only> pint;

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification on stack NOT allowed */
		nvobj::p<int, nvobj::Policy::pmem_only> pint(10);

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification on stack NOT allowed */
		nvobj::p<int, nvobj::Policy::pmem_only> pint(
			r->ptr_pmem->pchar);

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification inside of a transaction on pmem always allowed
		 */
		nvobj::transaction::run(pop, [&] {
			r->ptr_pmem->pint = 4;
			r->ptr_pmem->pint = r->ptr_pmem->pchar;

			r->ptr_pmem->pint += r->ptr_pmem->pchar;
			r->ptr_pmem->pint += char(10);
			r->ptr_pmem->pint += int(10);

			nvobj::delete_persistent<foo<nvobj::Policy::pmem_only>>(
				r->ptr_pmem);
		});
	} catch (...) {
		UT_ASSERT(0);
	}

	static_assert(
		std::is_same<
			pmem::obj::pmem_only::p<int>,
			pmem::obj::p<int, pmem::obj::Policy::pmem_only>>::value,
		"");
}

void
test_policy_tx(nvobj::pool<root> &pop)
{
	auto r = pop.root();

	try {
		nvobj::transaction::run(pop, [&] {
			r->ptr_tx = nvobj::make_persistent<
				foo<nvobj::Policy::tx_only>>();
		});
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification outside of a transaction NOT allowed */
		r->ptr_tx->pint = 10;

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification outside of a transaction NOT allowed */
		r->ptr_tx->pint = r->ptr_tx->pchar;

		UT_ASSERT(0);
	} catch (pmem::transaction_error &) {
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification on stack allowed */
		nvobj::p<int, nvobj::Policy::tx_only> pint;
		nvobj::p<int, nvobj::Policy::tx_only> pint2(10);
		nvobj::p<int, nvobj::Policy::tx_only> pint3(r->ptr_tx->pint);
	} catch (...) {
		UT_ASSERT(0);
	}

	try {
		/* modification inside of a transaction on pmem always allowed
		 */
		nvobj::transaction::run(pop, [&] {
			r->ptr_tx->pint = 4;

			r->ptr_tx->pint += r->ptr_tx->pchar;
			r->ptr_tx->pint += char(10);
			r->ptr_tx->pint += int(10);

			nvobj::delete_persistent<foo<nvobj::Policy::tx_only>>(
				r->ptr_tx);
		});
	} catch (...) {
		UT_ASSERT(0);
	}

	static_assert(
		std::is_same<
			pmem::obj::tx_only::p<int>,
			pmem::obj::p<int, pmem::obj::Policy::tx_only>>::value,
		"");
}
}

int
main(int argc, char *argv[])
{
	START();

	if (argc != 2)
		UT_FATAL("usage: %s file-name", argv[0]);

	const char *path = argv[1];

	nvobj::pool<struct root> pop;

	try {
		pop = nvobj::pool<struct root>::create(
			path, LAYOUT, PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR);
	} catch (pmem::pool_error &pe) {
		UT_FATAL("!pool::create: %s %s", pe.what(), path);
	}

	test_policy_weak(pop);
	test_policy_tx(pop);
	test_policy_restricted(pop);
	test_policy_pmem(pop);

	pop.close();

	return 0;
}
