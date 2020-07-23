// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "radix.hpp"

#include <random>

void
test_basic(nvobj::pool<root> &pop)
{
	auto r = pop.root();

	nvobj::transaction::run(pop, [&] {
		r->radix_int_int = nvobj::make_persistent<container_int_int>();
	});

	nvobj::experimental::actions_tx::run(pop.handle(), [&]{
		for (unsigned i = 0; i < 18; i++) {
			r->radix_int_int->try_emplace(i, i);
		}

		std::cout << *(r->radix_int_int) << std::endl;

		for (unsigned i = 18; i < 128; i++) {
			r->radix_int_int->try_emplace(i, i);
		}

		//UT_ASSERTeq(r->radix_int_int->size(), 128);

		return 0;
	});

	//UT_ASSERTeq(r->radix_int_int->size(), 128);

	//std::cout << *(r->radix_int_int) << std::endl;

	// for (unsigned i = 0; i < 1024; i++) {
	// 	UT_ASSERT(!r->radix_int_int->try_emplace(i, i).second);
	// }

	nvobj::transaction::run(pop, [&] {
		nvobj::delete_persistent<container_int_int>(r->radix_int_int);
	});

	UT_ASSERT(OID_IS_NULL(pmemobj_first(pop.handle())));
}

static void
test(int argc, char *argv[])
{
	if (argc != 2)
		UT_FATAL("usage: %s file-name", argv[0]);

	const char *path = argv[1];

	nvobj::pool<root> pop;

	try {
		pop = nvobj::pool<struct root>::create(path, "radix_basic",
						       10 * PMEMOBJ_MIN_POOL,
						       S_IWUSR | S_IRUSR);
	} catch (pmem::pool_error &pe) {
		UT_FATAL("!pool::create: %s %s", pe.what(), path);
	}

	test_basic(pop);

	pop.close();
}

int
main(int argc, char *argv[])
{
	return run_test([&] { test(argc, argv); });
}
