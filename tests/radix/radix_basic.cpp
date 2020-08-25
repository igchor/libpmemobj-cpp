// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "radix.hpp"

#include <algorithm>
#include <random>
#include <unordered_map>
#include <unordered_set>

#include <sstream>

static std::mt19937_64 generator;

const auto compressed_path_len = 4;
const auto num_children = 3;

static void
generate_compressed_tree(nvobj::persistent_ptr<container_string> ptr,
			 std::string prefix, int level)
{
	/* it should be just one digit */
	UT_ASSERT(num_children <= 9);

	if (!level)
		return;

	/* Test assumes characters are > 33 and < 122 (printable chars) */
	auto compressed_path =
		std::string(compressed_path_len, char(generator() % 87 + 34));
	for (int i = 0; i < num_children; i++) {
		auto key = prefix + std::to_string(i) + compressed_path;

		auto ret = ptr->try_emplace(key, "");
		UT_ASSERT(ret.second);

		generate_compressed_tree(ptr, key, level - 1);
	}
}

static void
verify_bounds(nvobj::persistent_ptr<container_string> ptr,
	      const std::vector<std::string> &keys)
{
	for (size_t i = 0; i < keys.size() - 1; i++) {
		/* generate key k for which k < keys[i] && k >= keys[i - 1] */
		auto k = keys[i];
		k[k.size() - 1]--;

		if (i > 0)
			UT_ASSERT(k > keys[i - 1]);

		auto it = ptr->upper_bound(k);
		UT_ASSERT(it->key() == keys[i]);

		it = ptr->lower_bound(k);
		UT_ASSERT(it->key() == keys[i]);
	}
}

static void
verify_bounds_key(nvobj::persistent_ptr<container_string> ptr,
		  const std::vector<std::string> &keys, const std::string &key)
{
	auto expected = std::lower_bound(keys.begin(), keys.end(), key);
	auto actual = ptr->lower_bound(key);
	UT_ASSERT((expected == keys.end() && actual == ptr->end()) ||
		  actual->key() == *expected);

	expected = std::upper_bound(keys.begin(), keys.end(), key);
	actual = ptr->upper_bound(key);
	UT_ASSERT((expected == keys.end() && actual == ptr->end()) ||
		  actual->key() == *expected);
}

void
test_compression(nvobj::pool<root> &pop)
{
	const auto num_levels = 3;

	auto r = pop.root();

	nvobj::transaction::run(pop, [&] {
		r->radix_str = nvobj::make_persistent<container_string>();
	});

	generate_compressed_tree(r->radix_str, "", num_levels);

	std::vector<std::string> keys;
	for (auto &e : (*r->radix_str))
		keys.emplace_back(e.key().data(), e.key().size());

	auto test_keys = keys;
	std::sort(test_keys.begin(), test_keys.end());
	UT_ASSERT(test_keys == keys);

	verify_bounds(r->radix_str, keys);

	for (size_t i = 1; i < keys.size() - 1; i++) {
		/* Key consists of segments like this:
		 * N-path-M-path ... where N, M is child number.
		 */
		auto k = keys[i];

		k = keys[i];
		auto idx = k.size() - compressed_path_len +
			(generator() % compressed_path_len);

		/* flip some bit at the end (part of a compression) */
		k[idx] = 0;
		verify_bounds_key(r->radix_str, keys, k);
		auto lb = r->radix_str->lower_bound(k);
		auto rb = r->radix_str->upper_bound(k);
		UT_ASSERT(lb == rb);
		UT_ASSERT(r->radix_str->find(keys[i]) == lb);

		k[idx] = std::numeric_limits<char>::max();
		verify_bounds_key(r->radix_str, keys, k);

		k = keys[i];
		k[1] = 0;
		verify_bounds_key(r->radix_str, keys, k);

		k = keys[i];
		k[1] = std::numeric_limits<char>::max();
		verify_bounds_key(r->radix_str, keys, k);

		k = keys[i] + "postfix";
		verify_bounds_key(r->radix_str, keys, k);

		k = keys[i].substr(0, k.size() - compressed_path_len - 1);
		verify_bounds_key(r->radix_str, keys, k);
	}

	nvobj::transaction::run(pop, [&] {
		nvobj::delete_persistent<container_string>(r->radix_str);
	});

	UT_ASSERT(OID_IS_NULL(pmemobj_first(pop.handle())));
}

/* This test inserts elements in range [0:2:2 * numeric_limits<uint16_t>::max()]
 */
void
test_binary_keys(nvobj::pool<root> &pop)
{
	auto r = pop.root();
	auto kv_f = [](unsigned i) { return i * 2; };

	nvobj::transaction::run(pop, [&] {
		r->radix_int_int = nvobj::make_persistent<container_int_int>();
	});

	/* Used for testing iterator stability. */
	std::unordered_map<unsigned, typename container_int_int::iterator> its;

	for (unsigned i = 2 * std::numeric_limits<uint16_t>::max(); i > 0;
	     i -= 2) {
		auto ret = r->radix_int_int->emplace(i - 2, i - 2);
		UT_ASSERT(ret.second);
		its.emplace(i - 2, ret.first);
	}

	verify_elements(r->radix_int_int, std::numeric_limits<uint16_t>::max(),
			kv_f, kv_f);

	for (unsigned i = 1; i < 2 * std::numeric_limits<uint16_t>::max() - 2U;
	     i += 2) {
		auto lit = r->radix_int_int->lower_bound(i);
		UT_ASSERT(lit->key() == i + 1);

		auto uit = r->radix_int_int->upper_bound(i);
		UT_ASSERT(uit->key() == i + 1);
	}

	/* Used for testing iterator stability. In each iteration one element
	 * is erased. This erasure should not affect further checks. */
	for (unsigned i = 2 * std::numeric_limits<uint16_t>::max(); i > 0;
	     i -= 2) {
		auto &it = its.find(i - 2)->second;
		UT_ASSERT(it->key() == i - 2);
		UT_ASSERT(it->value() == i - 2);

		r->radix_int_int->erase(i - 2);
	}

	nvobj::transaction::run(pop, [&] {
		nvobj::delete_persistent<container_int_int>(r->radix_int_int);
	});

	its = {};

	UT_ASSERT(OID_IS_NULL(pmemobj_first(pop.handle())));

	nvobj::transaction::run(pop, [&] {
		r->radix_int_int = nvobj::make_persistent<container_int_int>();
	});

	for (unsigned i = 0; i < 2 * std::numeric_limits<uint16_t>::max();
	     i += 2) {
		auto ret = r->radix_int_int->emplace(i, i);
		UT_ASSERT(ret.second);
		its.emplace(i, ret.first);
	}

	verify_elements(r->radix_int_int, std::numeric_limits<uint16_t>::max(),
			kv_f, kv_f);

	for (unsigned i = 1; i < 2 * std::numeric_limits<uint16_t>::max() - 2U;
	     i += 2) {
		auto lit = r->radix_int_int->lower_bound(i);
		UT_ASSERT(lit->key() == i + 1);

		auto uit = r->radix_int_int->upper_bound(i);
		UT_ASSERT(uit->key() == i + 1);
	}

	/* Used for testing iterator stability. In each iteration one element
	 * is erased. This erasure should not affect further checks. */
	for (unsigned i = 0; i < 2 * std::numeric_limits<uint16_t>::max();
	     i += 2) {
		auto &it = its.find(i)->second;
		UT_ASSERT(it->key() == i);
		UT_ASSERT(it->value() == i);

		r->radix_int_int->erase(i);
	}

	nvobj::transaction::run(pop, [&] {
		nvobj::delete_persistent<container_int_int>(r->radix_int_int);
	});

	UT_ASSERT(OID_IS_NULL(pmemobj_first(pop.handle())));
}

void
test_pre_post_fixes(nvobj::pool<root> &pop)
{
	auto num_elements = (1U << 10);

	std::vector<std::string> elements;
	elements.reserve(num_elements);

	elements.push_back("0");

	/* Used for testing iterator stability. */
	std::unordered_map<std::string, typename container_string::iterator>
		its;

	/* This loop creates string so that elements[i] is prefix of
	 * elements[i + 1] and they differ only by 4 bits:
	 * '0xA0', '0xA0 0xAB', '0xA0 0xAB 0xC0', '0xA0 0xAB 0xC0 0xCD'
	 */
	for (unsigned i = 1; i < num_elements * 2; i++) {
		if (i % 2 == 0)
			elements.push_back(
				elements.back() +
				std::string(1, char(generator() % 127 + 1)));
		else {
			auto str = elements.back();
			str.back() |= (-(char(generator() % 127 + 1)));
			elements.push_back(str);
		}
	}

	std::vector<std::string> s_elements = elements;
	std::sort(s_elements.begin(), s_elements.end());

	/* there might be some duplicates so update the total size */
	s_elements.erase(std::unique(s_elements.begin(), s_elements.end()),
			 s_elements.end());
	num_elements = s_elements.size();

	auto r = pop.root();

	nvobj::transaction::run(pop, [&] {
		r->radix_str = nvobj::make_persistent<container_string>();
	});

	for (unsigned i = elements.size(); i > 0; i--) {
		auto ret =
			r->radix_str->emplace(elements[i - 1], elements[i - 1]);
		if (ret.second)
			its.emplace(elements[i - 1], ret.first);
	}

	verify_bounds(r->radix_str, s_elements);

	UT_ASSERTeq(r->radix_str->size(), num_elements);
	unsigned i = 0;
	for (auto it = r->radix_str->begin(); it != r->radix_str->end();
	     ++it, ++i) {
		UT_ASSERT(nvobj::string_view(it->key()) == s_elements[i]);
	}

	/* Used for testing iterator stability. */
	for (unsigned i = num_elements; i > 0; i--) {
		auto &it = its.find(s_elements[i - 1])->second;
		UT_ASSERT(nvobj::string_view(it->key()).compare(
				  s_elements[i - 1]) == 0);
		UT_ASSERT(nvobj::string_view(it->value())
				  .compare(s_elements[i - 1]) == 0);
	}

	nvobj::transaction::run(pop, [&] {
		nvobj::delete_persistent<container_string>(r->radix_str);
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

	std::random_device rd;
	auto seed = rd();
	std::cout << "rand seed: " << seed << std::endl;
	generator = std::mt19937_64(seed);

	test_binary_keys(pop);
	test_pre_post_fixes(pop);
	test_compression(pop);

	pop.close();
}

int
main(int argc, char *argv[])
{
	return run_test([&] { test(argc, argv); });
}
