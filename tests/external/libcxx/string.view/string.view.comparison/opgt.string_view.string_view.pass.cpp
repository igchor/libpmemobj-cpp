//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//
//
// Copyright 2020, Intel Corporation
//
// Modified to test pmem::obj containers
//

// <string>

// template<class charT, class traits>
//  constexpr bool operator>(basic_string_view<charT,traits> lhs,
//                  basic_string_view<charT,traits> rhs);

#include "unittest.hpp"

#include <libpmemobj++/string_view.hpp>

template <class S>
void
test(const S &lhs, const S &rhs, bool x, bool y)
{
	UT_ASSERT((lhs > rhs) == x);
	UT_ASSERT((rhs > lhs) == y);
}

static void
run(int argc, char *argv[])
{
	{
		typedef pmem::obj::string_view S;
		test(S(""), S(""), false, false);
		test(S(""), S("abcde"), false, true);
		test(S(""), S("abcdefghij"), false, true);
		test(S(""), S("abcdefghijklmnopqrst"), false, true);
		test(S("abcde"), S(""), true, false);
		test(S("abcde"), S("abcde"), false, false);
		test(S("abcde"), S("abcdefghij"), false, true);
		test(S("abcde"), S("abcdefghijklmnopqrst"), false, true);
		test(S("abcdefghij"), S(""), true, false);
		test(S("abcdefghij"), S("abcde"), true, false);
		test(S("abcdefghij"), S("abcdefghij"), false, false);
		test(S("abcdefghij"), S("abcdefghijklmnopqrst"), false, true);
		test(S("abcdefghijklmnopqrst"), S(""), true, false);
		test(S("abcdefghijklmnopqrst"), S("abcde"), true, false);
		test(S("abcdefghijklmnopqrst"), S("abcdefghij"), true, false);
		test(S("abcdefghijklmnopqrst"), S("abcdefghijklmnopqrst"),
		     false, false);
	}

#ifdef XXX // XXX: Implement constexpr_char_traits
	{
		typedef std::basic_string_view<char,
					       constexpr_char_traits<char>>
			SV;
		constexpr SV sv1;
		constexpr SV sv2{"abcde", 5};

		static_assert(!(sv1 > sv1), "");
		static_assert(!(sv2 > sv2), "");

		static_assert(!(sv1 > sv2), "");
		static_assert(sv2 > sv1, "");
	}
#endif
}

int
main(int argc, char *argv[])
{
	return run_test([&] { run(argc, argv); });
}
