// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "transaction_helpers.hpp"
#include "unittest.hpp"

#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>

#include <functional>

#include <libpmemobj.h>

namespace nvobj = pmem::obj;
namespace nvobjex = pmem::obj::experimental;

template <typename T>
struct actions {
	actions(nvobj::pool_base p): pop(p) {

	}

	~actions() {
		if (acts.size()) {
			pmemobj_cancel(pop.handle(), acts.data(), acts.size());
		}
	}

	nvobj::persistent_ptr<T> reserve(size_t n) {
		acts.emplace_back();
		nvobj::persistent_ptr<T> ptr = pmemobj_reserve(pop.handle(), &acts.back(), n * sizeof(T), 0);

		if (ptr == nullptr) {
				throw std::bad_alloc{};
		}

		return ptr;
	}

	void publish() {
		auto ret = pmemobj_publish(pop.handle(), acts.data(), acts.size());
		if (ret)
			throw std::runtime_error("XXX");

		acts.clear();
	}

	void tx_publish() {
		auto ret = pmemobj_tx_publish(acts.data(), acts.size());
		if (ret)
			throw std::runtime_error("XXX");

		acts.clear();
	}

	std::vector<pobj_action> acts;
	nvobj::pool_base pop;
};

template <typename T>
struct intent_allocator {
	using value_type = T;
	using pointer = nvobj::persistent_ptr<T>;
	using const_pointer = nvobj::persistent_ptr<const T>;
	using reference = T&;
	using const_reference = const T&;
	using size_type = std::size_t;

	intent_allocator(actions<T> &acts): acts(acts) {

	}

	pointer allocate(size_t n, const_pointer = 0) {
		return acts.reserve(n);
	}

	actions<T> &acts;
};

using container_int =
	nvobjex::radix_tree<nvobjex::inline_string, nvobj::p<unsigned>>;
using container_string =
	nvobjex::radix_tree<nvobjex::inline_string, nvobjex::inline_string>;

using container_int_int =
	nvobjex::radix_tree<unsigned, nvobj::p<unsigned>>;
using container_int_string =
	nvobjex::radix_tree<unsigned, nvobjex::inline_string>;

using container_inline_s_wchart = nvobjex::radix_tree<nvobjex::basic_inline_string<wchar_t>, nvobj::p<unsigned>>;
using container_inline_s_wchart_wchart = nvobjex::radix_tree<nvobjex::basic_inline_string<wchar_t>, nvobjex::basic_inline_string<wchar_t>>;
using container_inline_s_u8t = nvobjex::radix_tree<nvobjex::basic_inline_string<uint8_t>, nvobjex::basic_inline_string<uint8_t>>;

struct root {
	nvobj::persistent_ptr<container_int> radix_int;
	nvobj::persistent_ptr<container_string> radix_str;

	nvobj::persistent_ptr<container_int_int> radix_int_int;
	nvobj::persistent_ptr<container_int_string> radix_int_str;

	nvobj::persistent_ptr<container_inline_s_wchart> radix_inline_s_wchart;
	nvobj::persistent_ptr<container_inline_s_wchart_wchart> radix_inline_s_wchart_wchart;
	nvobj::persistent_ptr<container_inline_s_u8t> radix_inline_s_u8t;
};

template <typename Container,
	  typename Enable = typename std::enable_if<std::is_same<
		  typename Container::mapped_type, nvobj::p<unsigned>>::value>::type>
typename Container::mapped_type
value(unsigned v, int repeats = 1)
{
	(void)repeats;
	return v;
}

template <typename Container,
	  typename Enable = typename std::enable_if<
		  std::is_same<typename Container::mapped_type,
			       typename nvobjex::basic_inline_string<typename Container::mapped_type::value_type>>::value>::type>
std::basic_string<typename Container::mapped_type::value_type>
value(unsigned v, int repeats = 1)
{
	using CharT = typename Container::mapped_type::value_type;

	auto s = std::basic_string<CharT>{};
	for (int i = 0; i < repeats; i++) {
		auto str = std::to_string(v);
		s += std::basic_string<CharT>(str.begin(), str.end());
	}

	return s;
}

template <typename Container,
	  typename Enable = typename std::enable_if<std::is_same<
		  typename Container::key_type, unsigned>::value>::type>
typename Container::key_type
key(unsigned v)
{
	return v;
}

template <typename Container,
	  typename Enable = typename std::enable_if<
		  std::is_same<typename Container::key_type,
			       typename nvobjex::basic_inline_string<typename Container::key_type::value_type>>::value>::type>
std::basic_string<typename Container::key_type::value_type>
key(unsigned v)
{
	using CharT = typename Container::key_type::value_type;

	auto s = std::to_string(v);

	return std::basic_string<CharT>(s.begin(), s.end());
}

template <typename CharT, typename Traits>
bool
operator==(pmem::obj::basic_string_view<CharT, Traits> lhs, const std::basic_string<CharT, Traits> &rhs)
{
	return lhs.compare(rhs) == 0;
}

template <typename CharT, typename Traits>
bool
operator==(pmem::obj::experimental::basic_inline_string<CharT, Traits> &lhs, const std::basic_string<CharT, Traits> &rhs)
{
	return pmem::obj::basic_string_view<CharT, Traits>(lhs.data(), lhs.size()).compare(rhs) == 0;
}

template <typename Container, typename K, typename F>
void
verify_elements(nvobj::persistent_ptr<Container> ptr, unsigned count, K&& key_f, F &&value_f)
{
	UT_ASSERTeq(ptr->size(), static_cast<size_t>(count));
	for (unsigned i = 0; i < count; i++) {
		auto it = ptr->find(key_f(i));
		UT_ASSERT(it->key() == key_f(i));
		UT_ASSERT(it->value() == value_f(i));

		auto lit = ptr->lower_bound(key_f(i));
		UT_ASSERT(lit->key() == key_f(i));
		UT_ASSERT(lit->value() == value_f(i));
	}

	std::vector<decltype(key_f(0))> keys;
	for (unsigned i = 0; i < count; i++)
		keys.emplace_back(key_f(i));
	std::sort(keys.begin(), keys.end());

	unsigned i = 0;
	for (auto it = ptr->begin(); it != ptr->end(); ++it, ++i) {
		UT_ASSERT(it->key() == keys[i]);
	}

	for (size_t i = 0; i < keys.size(); i++) {
		const auto &k = keys[i];

		auto uit = ptr->upper_bound(k);
		if (i == keys.size() - 1) {
			UT_ASSERT(uit == ptr->end());
		} else {
			UT_ASSERT(uit->key() == keys[i + 1]);
		}
	}
}
