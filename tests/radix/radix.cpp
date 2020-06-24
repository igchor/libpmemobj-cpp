// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "unittest.hpp"

#define private public

#include <libpmemobj++/experimental/radix.hpp>

namespace nvobj = pmem::obj;

using container = nvobj::radix_tree<int>;

struct root {
	nvobj::persistent_ptr<container> radix;
};

namespace
{
void
test_emplace(nvobj::pool<root> &pop)
{
	auto r = pop.root();

	nvobj::transaction::run(pop, [&] {
		r->radix = nvobj::make_persistent<container>();
		r->radix->emplace("", 0);
		r->radix->emplace("ab", 1);
		r->radix->emplace("ba", 2);
		r->radix->emplace("a", 3);
		r->radix->emplace("b", 4);
	});

	auto it = r->radix->find("a");
	UT_ASSERTeq(it->second, 3);

	++it;
	UT_ASSERTeq(it->second, 1);

	++it;
	UT_ASSERTeq(it->second, 4);

	++it;
	UT_ASSERTeq(it->second, 2);
}
}

static void
test(int argc, char *argv[])
{
	if (argc != 2)
		UT_FATAL("usage: %s file-name", argv[0]);

	const char *path = argv[1];

	nvobj::pool<root> pop;

	try {
		pop = nvobj::pool<struct root>::create(
			path, "XX", PMEMOBJ_MIN_POOL, S_IWUSR | S_IRUSR);
	} catch (pmem::pool_error &pe) {
		UT_FATAL("!pool::create: %s %s", pe.what(), path);
	}

	test_emplace(pop);

	pop.close();
}

int
main(int argc, char *argv[])
{
	return run_test([&] { test(argc, argv); });
}
