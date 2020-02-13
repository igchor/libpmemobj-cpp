/*
 * Copyright 2020, Intel Corporation
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
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

#include <libpmemobj/iterator_base.h>

namespace nvobj = pmem::obj;

using huge_object = char[1ULL << 30];

template <typename T>
struct simple_ptr
{
	simple_ptr()
	{
		ptr = nvobj::make_persistent<T>();
	}
	~simple_ptr()
	{
		UT_ASSERT(ptr != nullptr);

		nvobj::delete_persistent<T>(ptr);
	}

	nvobj::persistent_ptr<T> ptr;
};

struct C {
	C() : b()
	{
		nvobj::make_persistent<huge_object>();
	}

	simple_ptr<int> b;
};

struct C_nested {
	C_nested() : b()
	{
		nvobj::make_persistent<huge_object>();
	}

	simple_ptr<simple_ptr<int>> b;
};

struct root {
	nvobj::persistent_ptr<C> c_ptr;
	nvobj::persistent_ptr<C_nested> c_nested_ptr;

	nvobj::persistent_ptr<int> p1;
	nvobj::persistent_ptr<int> p2;
	nvobj::persistent_ptr<int> p3;
};

void
test_dtor_after_tx_abort(nvobj::pool<struct root> &pop)
{
	try {
		nvobj::transaction::run(pop, [&] {
			pop.root()->c_ptr = nvobj::make_persistent<C>();
		});

		UT_ASSERT(0);
	} catch (pmem::transaction_alloc_error &){
	} catch (std::exception &e) {
		UT_FATALexc(e);
	}
}

/*
 * Similar to test_dtor_after_tx_abort but with nested classes. In this case
 * even reading values in the nested object destructor is problematic (it is
 * done after memory reservation is cancelled so it's use after free).
 */
void
test_nested_dtor_after_tx_abort(nvobj::pool<struct root> &pop)
{
	try {
		nvobj::transaction::run(pop, [&] {
			pop.root()->c_nested_ptr = nvobj::make_persistent<C_nested>();
		});

		UT_ASSERT(0);
	} catch (pmem::transaction_alloc_error &){
	} catch (std::exception &e) {
		UT_FATALexc(e);
	}
}

void
test_memory_is_freed(nvobj::pool<struct root> &pop)
{
	auto r = pop.root();

	try {
		nvobj::transaction::run(pop, [&] {
			r->p1 = nvobj::make_persistent<int>();
			r->p2 = nvobj::make_persistent<int>();
			r->p3 = nvobj::make_persistent<int>();

			nvobj::make_persistent<huge_object>();
		});

		UT_ASSERT(0);
	} catch (pmem::transaction_alloc_error &){
	} catch (std::exception &e) {
		UT_FATALexc(e);
	}

	auto oid = pmemobj_first(pop.handle());
	UT_ASSERT(OID_IS_NULL(oid));
}

int
main(int argc, char *argv[])
{
	START();

	if (argc < 2) {
		std::cerr << "usage: " << argv[0] << " file-name" << std::endl;
		return 1;
	}

	auto path = argv[1];
	auto pop = nvobj::pool<root>::create(
		path, "error_handling", PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR);

	test_memory_is_freed(pop);

	test_dtor_after_tx_abort(pop);
	test_nested_dtor_after_tx_abort(pop);

	pop.close();

	return 0;
}
