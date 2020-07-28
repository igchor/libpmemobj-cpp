// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

/*
 * insert_open.cpp -- this simple benchmarks is used to measure time of
 * inserting specified number of elements and time of runtime_initialize().
 */

#include <cassert>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <libpmemobj++/experimental/radix_tree.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include "../measure.hpp"

#ifndef _WIN32

#include <unistd.h>
#define CREATE_MODE_RW (S_IWUSR | S_IRUSR)

#else

#include <windows.h>
#define CREATE_MODE_RW (S_IWRITE | S_IREAD)

#endif

static const std::string LAYOUT = "iteration";

using key_type = size_t;
using value_type = pmem::obj::p<size_t>;

using persistent_map_type =
	pmem::obj::experimental::radix_tree<key_type, value_type>;

struct root {
	pmem::obj::persistent_ptr<persistent_map_type> pptr;
};

void
insert(pmem::obj::pool<root> &pop, size_t n_inserts)
{
	auto map = pop.root()->pptr;

	assert(map != nullptr);

	pmem::obj::transaction::run(pop, [&] {
		for (size_t i = 0; i < n_inserts; i++) {
			auto ret = map->try_emplace(i, i);
			(void)ret;
			assert(ret.second);
		}
	});

	assert(map->size() == n_inserts);
}

int
main(int argc, char *argv[])
{
	pmem::obj::pool<root> pop;
	try {
		std::string usage = "usage: %s file-name <create n_inserts>";

		if (argc < 4) {
			std::cerr << usage << std::endl;
			return 1;
		}

		auto mode = std::string(argv[2]);

		if (mode != "create" && mode != "iterate") {
			std::cerr << usage << std::endl;
			return 1;
		}

		const char *path = argv[1];

		if (mode == "create") {
			size_t n_inserts = std::stoull(argv[3]);

			try {
				auto pool_size = 1000 * PMEMOBJ_MIN_POOL;

				pop = pmem::obj::pool<root>::create(
					path, LAYOUT, pool_size,
					CREATE_MODE_RW);
				pmem::obj::transaction::run(pop, [&] {
					pop.root()->pptr =
						pmem::obj::make_persistent<
							persistent_map_type>();
				});

				std::cout << measure<std::chrono::milliseconds>(
						     [&] {
							     insert(pop,
								    n_inserts);
						     })
					  << "ms" << std::endl;
			} catch (pmem::pool_error &pe) {
				std::cerr << "!pool::create: " << pe.what()
					  << std::endl;
				return 1;
			}
		} else {
			throw std::runtime_error("Wrong argv.");
		}

		pop.close();
	} catch (const std::logic_error &e) {
		std::cerr << "!pool::close: " << e.what() << std::endl;
		return 1;
	} catch (const std::exception &e) {
		std::cerr << "!exception: " << e.what() << std::endl;
		try {
			pop.close();
		} catch (const std::logic_error &e) {
			std::cerr << "!exception: " << e.what() << std::endl;
		}
		return 1;
	}
	return 0;
}
