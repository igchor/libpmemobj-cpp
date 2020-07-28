// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#ifndef TRANSACTION_HELPERS_HPP
#define TRANSACTION_HELPERS_HPP

#include "unittest.hpp"

#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <libpmemobj++/experimental/actions.hpp>

void
assert_tx_abort(pmem::obj::pool<struct root> &pop, std::function<void(void)> f)
{
	bool exception_thrown = false;
	try {
		pmem::obj::experimental::actions_tx::run(pop, [&] {
			f();
			throw pmem::manual_tx_abort("");
		});
	} catch (pmem::manual_tx_abort &) {
		exception_thrown = true;
	} catch (std::exception &e) {
		UT_FATALexc(e);
	}
	UT_ASSERT(exception_thrown);
}

#endif /* TRANSACTION_HELPERS_HPP */
