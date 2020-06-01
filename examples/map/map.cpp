// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <stdexcept>

using map_t = void /* XXX: pmem::obj::map<int, int> */;

struct root {
	pmem::obj::persistent_ptr<map_t> map;
};

int
main(int argc, char *argv[])
{
	if (argc < 2) {
		return 1;
	}

	const char *path = argv[1];

	auto pop = pmem::obj::pool<root>::open(path, "simplekv");
	auto r = pop.root();

	if (r->map == nullptr) {
		pmem::obj::transaction::run(pop, [&] {
			r->map = pmem::obj::make_persistent<map_t>();
		});
	}

    auto &map = *(r->map);

    pmem::obj::transaction::run(pop, [&]{
        map.insert({1, 1});
        map.insert({2, 2});
        map.insert({3, 3});
    });

    map.insert({4, 4});

    pmem::obj::transaction::run(pop, [&]{
        *map.find(1) = 2;
        *map.find(2) = 3;

        for (auto it = map.upper_bound(3); it != map.end(); it++)
            (*it)++;

        map.erase(3);
    });

    // ----------- only non-failing operations ------------

    actions act;
    acts.add(map.insert_or_assign(4, 4));
    acts.add(map.insert_or_assign(5, 5));

    for (auto it = map.upper_bound(3); it != map.end(); it++)
        acts.add(map.insert_or_assign(*it, *it + 1));

    act.publish();

    pop.close();
	return 0;
}
