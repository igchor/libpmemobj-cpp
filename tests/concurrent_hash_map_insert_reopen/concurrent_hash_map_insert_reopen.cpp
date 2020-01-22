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

#include "../concurrent_hash_map/concurrent_hash_map_test.hpp"
#include "unittest.hpp"

const size_t thread_items = 500000;
const size_t concurrency = 4;

void
insert(nvobj::pool<root> &pop)
{
	ConcurrentHashMapTestPrimitives test(pop, concurrency * thread_items);

	auto map = pop.root()->cons;

	UT_ASSERT(map != nullptr);

	map->runtime_initialize();

	parallel_exec(concurrency, [&](size_t thread_id) {
		int begin = thread_id * thread_items;
		int end = begin + int(thread_items);
		for (int i = begin; i < end; ++i) {
			persistent_map_type::value_type val(i, i);
			test.insert<persistent_map_type::accessor>(val);
		}
	});

	test.check_items_count();
}

void
check(nvobj::pool<root> &pop)
{
	ConcurrentHashMapTestPrimitives test(pop, concurrency * thread_items);

	auto map = pop.root()->cons;

	UT_ASSERT(map != nullptr);

	map->runtime_initialize();

	test.check_items_count();
}

int
main(int argc, char *argv[])
{
	START();

	if (argc < 2) {
		UT_FATAL("usage: %s file-name", argv[0]);
	}

	const char *path = argv[1];

	nvobj::pool<root> pop;

	if (std::string(argv[2]) == "c") {
		try {
			pop = nvobj::pool<root>::create(path, LAYOUT,
							(1ULL << 30) * 100,
							S_IWUSR | S_IRUSR);
			pmem::obj::transaction::run(pop, [&] {
				pop.root()->cons = nvobj::make_persistent<
					persistent_map_type>();
			});
		} catch (pmem::pool_error &pe) {
			UT_FATAL("!pool::create: %s %s", pe.what(), path);
		}

		insert(pop);
	} else {
		pop = nvobj::pool<root>::open(path, LAYOUT);

		check(pop);
	}

	pop.close();
	return 0;
}
