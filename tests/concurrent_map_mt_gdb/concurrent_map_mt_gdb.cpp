// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include <thread>
#include <iostream>
#include <vector>
#include <cassert>

void s0() {}
void s1() {}

int loop = 1;

void func(int id)
{
	if (id == 1)
		while(loop) {s1();}

	s0();

	assert(false);
}

int main()
{
	std::vector<std::thread> t;
	for (int i = 0; i < 2; i++) {
		t.emplace_back(func, i);
	}

	for (auto &e: t) {
		e.join();
	}
}
