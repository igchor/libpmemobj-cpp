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
	UT_ASSERT(it.key() == std::string("a"));
	UT_ASSERTeq(it.value(), 3);

	++it;
	UT_ASSERT(it.key() == std::string("ab"));
	UT_ASSERTeq(it.value(), 1);

	++it;
	UT_ASSERT(it.key() == std::string("b"));
	UT_ASSERTeq(it.value(), 4);

	++it;
	UT_ASSERT(it.key() == std::string("ba"));
	UT_ASSERTeq(it.value(), 2);

	--it;
	UT_ASSERT(it.key() == std::string("b"));
	UT_ASSERTeq(it.value(), 4);

	--it;
	UT_ASSERT(it.key() == std::string("ab"));
	UT_ASSERTeq(it.value(), 1);

	--it;
	UT_ASSERT(it.key() == std::string("a"));
	UT_ASSERTeq(it.value(), 3);

	--it;
	UT_ASSERT(it.key() == std::string(""));
	UT_ASSERTeq(it.value(), 0);

	it = r->radix->erase(it);
	UT_ASSERT(it.key() == std::string("a"));
	UT_ASSERTeq(it.value(), 3);
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

	std::cout << "digraph Radix {" << std::endl;

	test_emplace(pop);

	pop.root()->radix->iterate();

	std::cout << "}" << std::endl;

	pop.close();
}

int
main(int argc, char *argv[])
{
	return run_test([&] { test(argc, argv); });
}
