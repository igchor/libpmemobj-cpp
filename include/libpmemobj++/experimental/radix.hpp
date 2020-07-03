// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

/**
 * @file
 * Implementation of persistent radix tree.
 * Based on: https://github.com/pmem/pmdk/blob/master/src/libpmemobj/critnib.h
 */

#ifndef LIBPMEMOBJ_CPP_RADIX_HPP
#define LIBPMEMOBJ_CPP_RADIX_HPP

#include <libpmemobj++/allocator.hpp>
#include <libpmemobj++/inline_string.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/slice.hpp>
#include <libpmemobj++/string_view.hpp>
#include <libpmemobj++/transaction.hpp>

#include <algorithm>
#include <string>

#include <libpmemobj++/detail/pair.hpp>

#include <libpmemobj/action_base.h>

static uint64_t g_pool_id;

namespace pmem
{

namespace obj
{

struct actions {

	struct wrapper {
		uint64_t* what;
		uint64_t value;
	};

	actions(obj::pool_base pop, uint64_t pool_id, std::size_t cap = 4)
	    : pop(pop), pool_id(pool_id)
	{
		acts.reserve(cap);
	}

	void
	set(wrapper w, uint64_t value)
	{
		auto it = wal_v.find(w.what);
		if (it != wal_v.end())
			it->second = value;
		else
			wal_v.emplace(w.what, value);
	}

	template <typename T>
	wrapper get(T *what)
	{
		auto it = wal_v.find((uint64_t*)what);
		if (it != wal_v.end())
			return {(uint64_t*)what, it->second};

		return {(uint64_t*)what, what ? *((uint64_t*)what) : 0};
	}

	void
	free(uint64_t off)
	{
		off = off & ~1ULL;

		if (!off)
			return;

		acts.emplace_back();
		pmemobj_defer_free(pop.handle(), PMEMoid{pool_id, off},
				   &acts.back());
	}

	template <typename T, typename... Args>
	obj::persistent_ptr<T>
	make(uint64_t size, Args &&... args)
	{
		acts.emplace_back();
		obj::persistent_ptr<T> ptr =
			pmemobj_reserve(pop.handle(), &acts.back(), size, 0);

		new (ptr.get()) T(std::forward<Args>(args)...);

		return ptr;
	}

	void
	publish()
	{
		acts.reserve(acts.size() + wal_v.size());
		for (auto &v : wal_v) {
			acts.emplace_back();
			pmemobj_set_value(pop.handle(), &acts.back(), v.first, v.second);
		}

		if (pmemobj_publish(pop.handle(), acts.data(), acts.size()))
			throw std::runtime_error("XXX");
	}

private:
	std::vector<pobj_action> acts;
	std::unordered_map<uint64_t*, uint64_t> wal_v;
	obj::pool_base pop;
	uint64_t pool_id;
};

template <typename Value>
class radix_tree {
public:
	using key_type = inline_string;
	using mapped_type = Value;

	struct iterator;

	/* Default ctor - constructs empty tree */
	radix_tree();

	/* Dtor - removes entire tree */
	~radix_tree();

	template <class... Args>
	void emplace(actions &acts, string_view k, Args &&... args);

	iterator find(string_view k);

	iterator erase(iterator pos);

	iterator lower_bound(string_view k);

	iterator upper_bound(string_view k);

	iterator begin();

	iterator end();

	/*
	 * size -- return number of elements
	 */
	uint64_t size();

	/* prints tree in DOT format - used for debugging */
	void print();

private:
	static constexpr std::size_t SLICE = 4;
	static constexpr std::size_t NIB = ((1ULL << SLICE) - 1);
	static constexpr std::size_t SLNODES = (1 << SLICE);

	using byten_t = uint32_t;
	using bitn_t = uint8_t;

	struct tagged_node_ptr;
	struct node;

	struct leaf;

	/*** pmem members ***/
	tagged_node_ptr root;
	tagged_node_ptr shadow_root;
	p<uint64_t> size_;

	/* helper functions */
	static int n_child(tagged_node_ptr n);

	static tagged_node_ptr &parent_ref(tagged_node_ptr n);

	static bool keys_equal(string_view k1, string_view k2);

	template <typename ChildIterator>
	static tagged_node_ptr next(tagged_node_ptr n);

	template <typename ChildIterator>
	static tagged_node_ptr next_node(ChildIterator child_slot);

	template <typename ChildIterator>
	static tagged_node_ptr next_leaf(tagged_node_ptr n);

	/*
	 * internal: slice_index -- return index of child at the given nib
	 */
	static unsigned slice_index(char k, uint8_t shift);

	static byten_t prefix_diff(string_view lhs, string_view rhs);

	template <typename... Args>
	tagged_node_ptr make_leaf(actions &acts, tagged_node_ptr parent,
				  Args &&... args);

	leaf *bottom_leaf(actions &acts, tagged_node_ptr n);

	leaf *descend(actions &acts, string_view key);

	void print_rec(radix_tree::tagged_node_ptr n);

	/*
	 * internal: delete_node -- recursively free (to malloc) a subtree
	 */
	void delete_node(tagged_node_ptr n);
};

template <typename Value>
struct radix_tree<Value>::tagged_node_ptr {
	tagged_node_ptr(uint64_t off): off(off) {

	}

	tagged_node_ptr();
	tagged_node_ptr(const tagged_node_ptr &rhs);
	tagged_node_ptr(std::nullptr_t);

	tagged_node_ptr(const persistent_ptr<leaf> &ptr);
	tagged_node_ptr(const persistent_ptr<node> &ptr);

	tagged_node_ptr &operator=(const tagged_node_ptr &rhs);
	tagged_node_ptr &operator=(std::nullptr_t);
	tagged_node_ptr &operator=(const persistent_ptr<leaf> &rhs);
	tagged_node_ptr &operator=(const persistent_ptr<node> &rhs);

	bool operator==(const tagged_node_ptr &rhs) const;
	bool operator!=(const tagged_node_ptr &rhs) const;

	bool is_leaf() const;

	radix_tree::leaf *get_leaf() const;
	radix_tree::node *get_node() const;

	uint64_t
	offset() const
	{
		return off;
	}

	radix_tree::node *operator->() const noexcept;

	explicit operator bool() const noexcept;

private:
	p<uint64_t> off;
};

template <typename Value>
struct radix_tree<Value>::leaf {
	using tree_type = radix_tree<Value>;

	template <typename... Args>
	static persistent_ptr<leaf> make(actions &acts, tagged_node_ptr parent,
					 Args &&... args);

	string_view key();

	template <typename T = Value>
	typename std::enable_if<!std::is_same<T, inline_string>::value,
				mapped_type &>::type
	value();

	template <typename T = Value>
	typename std::enable_if<std::is_same<T, inline_string>::value,
				string_view>::type
	value();

	typename key_type::accessor key_accessor();

	template <typename T = Value>
	typename std::enable_if<std::is_same<T, inline_string>::value,
				typename T::accessor>::type
	value_accessor();

	tagged_node_ptr parent = nullptr;
	detail::pair<key_type, mapped_type> data;

	// private:
	template <typename... Args, typename T = Value,
		  typename Enable = typename std::enable_if<
			  !std::is_same<T, inline_string>::value>::type>
	leaf(string_view key, Args &&... args);

	template <typename T = Value,
		  typename Enable = typename std::enable_if<
			  std::is_same<T, inline_string>::value>::type>
	leaf(string_view key, string_view value);

	template <typename... Args, typename T = Value>
	static typename std::enable_if<!std::is_same<T, inline_string>::value,
				       persistent_ptr<leaf>>::type
	make_internal(actions &acts, string_view key, Args &&... args);

	template <typename T = Value>
	static typename std::enable_if<std::is_same<T, inline_string>::value,
				       persistent_ptr<leaf>>::type
	make_internal(actions &acts, string_view key, string_view value);
};

/*
 * Internal nodes store SLNODES children ptrs + one leaf ptr.
 * The leaf ptr is used only for nodes for which length of the path from
 * root is a multiple of byte (n->bit == 8 - SLICE).
 *
 * This ptr stores pointer to a leaf
 * which is a prefix of some other (bottom) leaf.
 */
template <typename Value>
struct radix_tree<Value>::node {
	tagged_node_ptr parent;
	tagged_node_ptr leaf;
	tagged_node_ptr child[SLNODES];
	byten_t byte;
	bitn_t bit;

	struct forward_iterator;
	struct reverse_iterator;

	template <typename Iterator>
	typename std::enable_if<std::is_same<Iterator, forward_iterator>::value,
				Iterator>::type
	begin();

	template <typename Iterator>
	typename std::enable_if<std::is_same<Iterator, forward_iterator>::value,
				Iterator>::type
	end();

	/* rbegin */
	template <typename Iterator>
	typename std::enable_if<std::is_same<Iterator, reverse_iterator>::value,
				Iterator>::type
	begin();

	/* rend */
	template <typename Iterator>
	typename std::enable_if<std::is_same<Iterator, reverse_iterator>::value,
				Iterator>::type
	end();

	template <typename Iterator = forward_iterator>
	Iterator find_child(tagged_node_ptr n);

	uint8_t padding[256 - sizeof(parent) - sizeof(leaf) - sizeof(child) -
			sizeof(byte) - sizeof(bit)];
};

template <typename Value>
struct radix_tree<Value>::iterator {
	using difference_type = std::ptrdiff_t;
	using mapped_reference =
		decltype(std::declval<radix_tree<Value>::leaf>().value());
	using value_type = std::pair<string_view, mapped_reference>;
	using pointer = value_type *;
	using reference = value_type &;
	using iterator_category = std::bidirectional_iterator_tag;

	iterator(radix_tree *, tagged_node_ptr);

	value_type operator*();

	string_view key();
	mapped_reference value();

	template <typename... Args, typename T = Value>
	typename std::enable_if<std::is_same<T, inline_string>::value>::type
	assign(string_view v);

	iterator operator++();
	iterator operator--();

	bool operator!=(const iterator &rhs);
	bool operator==(const iterator &rhs);

private:
	friend class radix_tree;

	tagged_node_ptr node;
	radix_tree *tree;
};

template <typename Value>
struct radix_tree<Value>::node::forward_iterator {
	using difference_type = std::ptrdiff_t;
	using value_type = tagged_node_ptr;
	using pointer = value_type *;
	using reference = value_type &;
	using iterator_category = std::input_iterator_tag;

	forward_iterator(tagged_node_ptr *ptr, node *n);

	forward_iterator operator++();
	forward_iterator operator++(int);

	tagged_node_ptr &operator*();
	tagged_node_ptr *operator->();

	node *get_node() const;

	bool operator!=(const forward_iterator &rhs) const;

private:
	tagged_node_ptr *ptr;
	node *n;
};

template <typename Value>
struct radix_tree<Value>::node::reverse_iterator {
	using difference_type = std::ptrdiff_t;
	using value_type = tagged_node_ptr;
	using pointer = value_type *;
	using reference = value_type &;
	using iterator_category = std::input_iterator_tag;

	reverse_iterator(tagged_node_ptr *ptr, node *n);

	reverse_iterator operator++();
	reverse_iterator operator++(int);

	tagged_node_ptr &operator*();
	tagged_node_ptr *operator->();

	node *get_node() const;

	bool operator!=(const reverse_iterator &rhs) const;

private:
	tagged_node_ptr *ptr;
	node *n;
};

template <typename Value>
radix_tree<Value>::radix_tree() : root(nullptr), size_(0)
{
	static_assert(sizeof(node) == 256,
		      "Internal node should have size equal to 256 bytes.");

	g_pool_id = pmemobj_oid(this).pool_uuid_lo;
}

template <typename Value>
radix_tree<Value>::~radix_tree()
{
	if (root)
		delete_node(root);
}

template <typename Value>
uint64_t
radix_tree<Value>::size()
{
	return this->size_;
}

/**
 * Find a bottom (leftmost) leaf in a subtree.
 */
template <typename Value>
typename radix_tree<Value>::leaf *
radix_tree<Value>::bottom_leaf(actions &acts, typename radix_tree<Value>::tagged_node_ptr n)
{
	for (size_t i = 0; i < SLNODES; i++) {
		auto m = tagged_node_ptr(acts.get(&n->child[i]).value);
		if (m)
			return m.is_leaf() ? m.get_leaf() : bottom_leaf(acts, m);
	}

	assert(false);
}

template <typename Value>
int
radix_tree<Value>::n_child(tagged_node_ptr n)
{
	int num = 0;
	for (int i = 0; i < (int)SLNODES; i++) {
		auto &child = n->child[i];
		if (child) {
			num++;
		}
	}

	return num;
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
radix_tree<Value>::parent_ref(tagged_node_ptr n)
{
	if (n.is_leaf())
		return n.get_leaf()->parent;

	return n->parent;
}

template <typename Value>
template <typename... Args>
typename radix_tree<Value>::tagged_node_ptr
radix_tree<Value>::make_leaf(actions &acts, tagged_node_ptr parent,
			     Args &&... args)
{
	acts.set({(uint64_t *)&size_, size_}, size_ + 1);
	return leaf::make(acts, parent, std::forward<Args>(args)...);
}

template <typename Value>
typename radix_tree<Value>::leaf *
radix_tree<Value>::descend(actions &acts, string_view key)
{
	auto n = tagged_node_ptr(acts.get(&root).value);

	while (!n.is_leaf() && n->byte < key.size()) {
		auto nn = tagged_node_ptr(acts.get(&n->child[slice_index(key.data()[n->byte], n->bit)]).value);

		if (nn)
			n = nn;
		else {
			n = bottom_leaf(acts, n);
			break;
		}
	}

	if (!n.is_leaf())
		n = bottom_leaf(acts, n);

	return n.get_leaf();
}

template <typename Value>
bool
radix_tree<Value>::keys_equal(string_view k1, string_view k2)
{
	return k1.size() == k2.size() && k1.compare(k2) == 0;
}

template <typename Value>
typename radix_tree<Value>::byten_t
radix_tree<Value>::prefix_diff(string_view lhs, string_view rhs)
{
	byten_t diff;
	for (diff = 0; diff < std::min(lhs.size(), rhs.size()); diff++) {
		if (lhs.data()[diff] != rhs.data()[diff])
			return diff;
	}

	return diff;
}

template <typename Value>
template <class... Args>
void
radix_tree<Value>::emplace(actions &acts, string_view key, Args &&... args)
{
	auto pop = pool_base(pmemobj_pool_by_ptr(this));

	auto sh_root = acts.get(&root);

	if (!tagged_node_ptr(sh_root.value)) {
		tagged_node_ptr new_leaf = make_leaf(
			acts, nullptr, key, std::forward<Args>(args)...);

		acts.set(sh_root, new_leaf.offset());

		return;
	}

	/*
	 * Need to descend the tree twice: first to find a leaf that
	 * represents a subtree whose all keys share a prefix at least as
	 * long as the one common to the new key and that subtree.
	 */
	auto leaf = descend(acts, key);
	auto diff = prefix_diff(key, leaf->key());

	/* Descend into the tree again. */
	auto n = sh_root;
	auto prev = n;

	auto min_key_len = std::min(leaf->key().size(), key.size());

	bitn_t sh = 8 - SLICE;
	if (diff < leaf->key().size() && diff < key.size()) {
		auto at = static_cast<unsigned char>(leaf->key().data()[diff] ^
						     key.data()[diff]);
		sh = detail::mssb_index((uint32_t)at) & (bitn_t) ~(SLICE - 1);
	}

	auto nptr = tagged_node_ptr(n.value);

	while (nptr && !nptr.is_leaf() &&
	       (nptr->byte < diff ||
		(nptr->byte == diff &&
		 (nptr->bit > sh || (nptr->bit == sh && diff < min_key_len))))) {

		prev = n;
		n = acts.get(&nptr->child[slice_index(key.data()[nptr->byte], nptr->bit)]);

		nptr = tagged_node_ptr(n.value);
	}

	/*
	 * If the divergence point is at same nib as an existing node, and
	 * the subtree there is empty, just place our leaf there and we're
	 * done.  Obviously this can't happen if SLICE == 1.
	 */
	if (!nptr) {
		assert(diff < leaf->key().size() && diff < key.size());

		tagged_node_ptr new_leaf =
			make_leaf(acts, prev.value, key, std::forward<Args>(args)...);

		acts.set(n, new_leaf.offset());

		return;
	}

	/* New key is a prefix of the leaf key or they are equal. We need to add
	 * leaf ptr to internal node. */
	if (diff == key.size()) {
		if (nptr.is_leaf() && nptr.get_leaf()->key().size() == key.size()) {
			return;
		}

		if (!nptr.is_leaf() && nptr->byte == key.size() &&
		    nptr->bit == 8 - SLICE) {
			auto l = acts.get(&nptr->leaf);
			if (tagged_node_ptr(l.value))
				return;

			tagged_node_ptr new_leaf = make_leaf(
				acts, n.value, key, std::forward<Args>(args)...);

			acts.set(l, new_leaf.offset());

			return;
		}

		tagged_node_ptr node =
			acts.make<radix_tree::node>(sizeof(radix_tree::node));

		tagged_node_ptr new_leaf =
			make_leaf(acts, node, key, std::forward<Args>(args)...);

		node->child[slice_index(leaf->key().data()[diff], sh)] = tagged_node_ptr(n.value);
		node->leaf = new_leaf;
		node->byte = diff;
		node->bit = sh;
		node->parent = acts.get(&parent_ref(n.value)).value;

		acts.set(n, node.offset());
		acts.set(acts.get(&parent_ref(n.value)), node.offset());

		return;
	}

	if (diff == leaf->key().size()) {
		/* Leaf key is a prefix of the new key. We need to convert leaf
		 * to a node.
		 */
		tagged_node_ptr node =
			acts.make<radix_tree::node>(sizeof(radix_tree::node));

		tagged_node_ptr new_leaf =
			make_leaf(acts, node, key, std::forward<Args>(args)...);

		node->child[slice_index(key.data()[diff], sh)] = new_leaf;
		node->leaf = n.value;
		node->byte = diff;
		node->bit = sh;
		node->parent = acts.get(&parent_ref(n.value)).value;

		acts.set(n, node.offset());
		acts.set(acts.get(&parent_ref(n.value)), node.offset());

		return;
	}

	tagged_node_ptr node =
		acts.make<radix_tree::node>(sizeof(radix_tree::node));

	tagged_node_ptr new_leaf =
		make_leaf(acts, node, key, std::forward<Args>(args)...);

	node->child[slice_index(leaf->key().data()[diff], sh)] = n.value;
	node->child[slice_index(key.data()[diff], sh)] = new_leaf;
	node->byte = diff;
	node->bit = sh;
	node->parent = acts.get(&parent_ref(n.value)).value;

	acts.set(n, node.offset());
	acts.set(acts.get(&parent_ref(n.value)), node.offset());

	return;
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::find(string_view key)
{
	auto n = root;
	while (n && !n.is_leaf()) {
		if (n->byte == key.size() && n->bit == 8 - SLICE)
			n = n->leaf;
		else if (n->byte > key.size())
			return end();
		else
			n = n->child[slice_index(key.data()[n->byte], n->bit)];
	}

	if (!n)
		return end();

	if (!keys_equal(key, n.get_leaf()->key()))
		return end();

	return iterator(this, n);
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::erase(iterator pos)
{
	auto pop = pool_base(pmemobj_pool_by_ptr(this));

	transaction::run(pop, [&] {
		auto leaf = pos.node.get_leaf();
		auto parent = leaf->parent;

		delete_persistent<radix_tree::leaf>(leaf);

		size_--;

		/* was root */
		if (!parent) {
			root = nullptr;
			pos = begin();
			return;
		}

		++pos;
		*parent->find_child(tagged_node_ptr(leaf)) = nullptr;

		/* Compress the tree vertically. */
		auto n = parent;
		parent = n->parent;
		tagged_node_ptr only_child = nullptr;
		for (size_t i = 0; i < SLNODES; i++) {
			if (n->child[i]) {
				if (only_child) {
					/* more than one child */
					return;
				}
				only_child = n->child[i];
			}
		}

		if (only_child && n->leaf) {
			/* There are actually 2 "childred" so we can't compress.
			 */
			return;
		} else if (n->leaf)
			only_child = n->leaf;

		assert(only_child);

		parent_ref(only_child) = n->parent;

		if (!parent)
			root = only_child;
		else
			*parent->find_child(n) = only_child;

		delete_persistent<radix_tree::node>(n.get_node());
	});

	return pos;
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::lower_bound(string_view key)
{
	auto n = root;
	decltype(n) prev;
	decltype(n) *slot = nullptr;

	if (!root)
		return end();

	while (n && !n.is_leaf()) {
		prev = n;

		if (n->byte == key.size() && n->bit == 8 - SLICE)
			slot = &n->leaf;
		else if (n->byte > key.size())
			return iterator(
				this,
				next_leaf<typename node::forward_iterator>(n));
		else
			slot = &n->child[slice_index(key.data()[n->byte],
						     n->bit)];

		n = *slot;
	}

	if (!n) {
		auto slot_it =
			typename node::forward_iterator(slot, prev.get_node());
		return iterator(this, next_node(slot_it));
	}

	assert(n.is_leaf());

	if (n.get_leaf()->key().compare(key) >= 0)
		return iterator(this, n);

	return ++iterator(this, n);
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::upper_bound(string_view k)
{
	auto it = lower_bound(k);

	// XXX: optimize -> it's 2 comparisons
	if (keys_equal(it.key(), k))
		return ++it;

	return it;
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::begin()
{
	if (!root)
		return iterator(this, nullptr);

	return iterator(
		this,
		radix_tree::next_leaf<
			typename radix_tree::node::forward_iterator>(root));
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::end()
{
	return iterator(this, nullptr);
}

template <typename Value>
void
radix_tree<Value>::print_rec(radix_tree::tagged_node_ptr n)
{
	if (!n.is_leaf()) {
		auto parent = n->parent ? n->parent.get_node() : nullptr;
		std::cout << (uint64_t)parent << " -> "
			  << (uint64_t)n.get_node() << std::endl;
		assert(n_child(n) + bool((n)->leaf) > 1);

		if (n->leaf)
			print_rec(n->leaf);

		for (int i = 0; i < (int)SLNODES; i++) {
			if (n->child[i])
				print_rec(n->child[i]);
		}
	} else {
		auto parent = n.get_leaf()->parent
			? n.get_leaf()->parent.get_node()
			: nullptr;
		std::cout << (uint64_t)parent << " -> "
			  << (uint64_t)n.get_leaf() << " [label=\""
			  << n.get_leaf()->key().data() << "\"]" << std::endl;

		if (parent && n == parent->leaf) {
			std::cout << "{rank=same!" << (uint64_t)parent << "!"
				  << (uint64_t)n.get_leaf() << "}" << std::endl;
		}
	}
}

template <typename Value>
void
radix_tree<Value>::print()
{
	std::cout << "digraph Radix {" << std::endl;

	if (root)
		print_rec(root);

	std::cout << "}" << std::endl;
}

template <typename Value>
unsigned
radix_tree<Value>::slice_index(char b, uint8_t bit)
{
	return static_cast<unsigned>(b >> bit) & NIB;
}

template <typename Value>
void
radix_tree<Value>::delete_node(radix_tree::tagged_node_ptr n)
{
	assert(pmemobj_tx_stage() == TX_STAGE_WORK);

	if (!n.is_leaf()) {
		if (n->leaf)
			delete_node(n->leaf);

		for (int i = 0; i < (int)SLNODES; i++) {
			if (n->child[i])
				delete_node(n.get_node()->child[i]);
		}
		delete_persistent<radix_tree::node>(n.get_node());
	} else {
		size_--;
		delete_persistent<radix_tree::leaf>(n.get_leaf());
	}
}

template <typename Value>
radix_tree<Value>::tagged_node_ptr::tagged_node_ptr(std::nullptr_t)
{
	this->off = 0;
}

template <typename Value>
radix_tree<Value>::tagged_node_ptr::tagged_node_ptr()
{
	this->off = 0;
}

template <typename Value>
radix_tree<Value>::tagged_node_ptr::tagged_node_ptr(const tagged_node_ptr &rhs)
{
	this->off = rhs.off;
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
radix_tree<Value>::tagged_node_ptr::operator=(const tagged_node_ptr &rhs)
{
	this->off = rhs.off;
	return *this;
}

template <typename Value>
radix_tree<Value>::tagged_node_ptr::tagged_node_ptr(
	const persistent_ptr<leaf> &ptr)
{
	off = (ptr.raw().off | 1);
}

template <typename Value>
radix_tree<Value>::tagged_node_ptr::tagged_node_ptr(
	const persistent_ptr<node> &ptr)
{
	off = ptr.raw().off;
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
	radix_tree<Value>::tagged_node_ptr::operator=(std::nullptr_t)
{
	this->off = 0;

	return *this;
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
radix_tree<Value>::tagged_node_ptr::operator=(const persistent_ptr<leaf> &rhs)
{
	off = (rhs.raw().off | 1);
	return *this;
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
radix_tree<Value>::tagged_node_ptr::operator=(const persistent_ptr<node> &rhs)
{
	off = rhs.raw().off;
	return *this;
}

template <typename Value>
bool
radix_tree<Value>::tagged_node_ptr::operator==(
	const radix_tree::tagged_node_ptr &rhs) const
{
	return off == rhs.off;
}

template <typename Value>
bool
radix_tree<Value>::tagged_node_ptr::operator!=(
	const radix_tree::tagged_node_ptr &rhs) const
{
	return !(*this == rhs);
}

template <typename Value>
bool
radix_tree<Value>::tagged_node_ptr::is_leaf() const
{
	return off & 1U;
}

template <typename Value>
typename radix_tree<Value>::leaf *
radix_tree<Value>::tagged_node_ptr::get_leaf() const
{
	assert(is_leaf());
	return (leaf *)pmemobj_direct({g_pool_id, off & ~1ULL});
}

template <typename Value>
typename radix_tree<Value>::node *
radix_tree<Value>::tagged_node_ptr::get_node() const
{
	assert(!is_leaf());
	return (node *)pmemobj_direct({g_pool_id, off});
}

template <typename Value>
radix_tree<Value>::tagged_node_ptr::operator bool() const noexcept
{
	return (off & ~1ULL) != 0;
}

template <typename Value>
typename radix_tree<Value>::node *
	radix_tree<Value>::tagged_node_ptr::operator->() const noexcept
{
	return get_node();
}

template <typename Value>
radix_tree<Value>::node::forward_iterator::forward_iterator(
	tagged_node_ptr *ptr, node *n)
    : ptr(ptr), n(n)
{
}

template <typename Value>
typename radix_tree<Value>::node::forward_iterator
radix_tree<Value>::node::forward_iterator::operator++()
{
	if (ptr == &n->leaf)
		ptr = &n->child[0];
	else
		ptr++;

	return *this;
}

template <typename Value>
typename radix_tree<Value>::node::forward_iterator
radix_tree<Value>::node::forward_iterator::operator++(int)
{
	forward_iterator tmp(ptr, n);
	operator++();
	return tmp;
}
template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
	radix_tree<Value>::node::forward_iterator::operator*()
{
	return *ptr;
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr *
	radix_tree<Value>::node::forward_iterator::operator->()
{
	return ptr;
}

template <typename Value>
typename radix_tree<Value>::node *
radix_tree<Value>::node::forward_iterator::get_node() const
{
	return n;
}

template <typename Value>
bool
radix_tree<Value>::node::forward_iterator::operator!=(
	const forward_iterator &rhs) const
{
	return ptr != rhs.ptr;
}

template <typename Value>
radix_tree<Value>::node::reverse_iterator::reverse_iterator(
	tagged_node_ptr *ptr, node *n)
    : ptr(ptr), n(n)
{
}

template <typename Value>
typename radix_tree<Value>::node::reverse_iterator
radix_tree<Value>::node::reverse_iterator::operator++()
{
	if (ptr == &n->child[0])
		ptr = &n->leaf;
	else
		ptr--;

	return *this;
}

template <typename Value>
typename radix_tree<Value>::node::reverse_iterator
radix_tree<Value>::node::reverse_iterator::operator++(int)
{
	reverse_iterator tmp(ptr, n);
	operator++();
	return tmp;
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
	radix_tree<Value>::node::reverse_iterator::operator*()
{
	return *ptr;
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr *
	radix_tree<Value>::node::reverse_iterator::operator->()
{
	return ptr;
}

template <typename Value>
bool
radix_tree<Value>::node::reverse_iterator::operator!=(
	const reverse_iterator &rhs) const
{
	return ptr != rhs.ptr;
}

template <typename Value>
typename radix_tree<Value>::node *
radix_tree<Value>::node::reverse_iterator::get_node() const
{
	return n;
}

template <typename Value>
template <typename Iterator>
typename std::enable_if<
	std::is_same<Iterator,
		     typename radix_tree<Value>::node::forward_iterator>::value,
	Iterator>::type
radix_tree<Value>::node::begin()
{
	return Iterator(&leaf, this);
}

template <typename Value>
template <typename Iterator>
typename std::enable_if<
	std::is_same<Iterator,
		     typename radix_tree<Value>::node::forward_iterator>::value,
	Iterator>::type
radix_tree<Value>::node::end()
{
	return Iterator(&child[SLNODES], this);
}

template <typename Value>
template <typename Iterator>
typename std::enable_if<
	std::is_same<Iterator,
		     typename radix_tree<Value>::node::reverse_iterator>::value,
	Iterator>::type
radix_tree<Value>::node::begin()
{
	return Iterator(&child[SLNODES - 1], this);
}

template <typename Value>
template <typename Iterator>
typename std::enable_if<
	std::is_same<Iterator,
		     typename radix_tree<Value>::node::reverse_iterator>::value,
	Iterator>::type
radix_tree<Value>::node::end()
{
	return Iterator(&leaf - 1, this);
}

template <typename Value>
template <typename Iterator>
Iterator
radix_tree<Value>::node::find_child(radix_tree<Value>::tagged_node_ptr n)
{
	return std::find(begin<Iterator>(), end<Iterator>(), n);
}

template <typename Value>
radix_tree<Value>::iterator::iterator(
	radix_tree *t, typename radix_tree<Value>::tagged_node_ptr node)
    : node(node), tree(t)
{
}

template <typename Value>
typename radix_tree<Value>::iterator::value_type
	radix_tree<Value>::iterator::operator*()
{
	auto leaf = node.get_leaf();

	return {leaf->key(), leaf->value()};
}

template <typename Value>
string_view
radix_tree<Value>::iterator::key()
{
	return (**this).first;
}

template <typename Value>
typename radix_tree<Value>::iterator::mapped_reference
radix_tree<Value>::iterator::value()
{
	return node.get_leaf()->value();
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::iterator::operator++()
{
	if (node)
		node = next<typename node::forward_iterator>(node);

	return *this;
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::iterator::operator--()
{
	if (node) // XXX - what if node == nullptr (end())
		node = next<typename node::reverse_iterator>(node);

	return *this;
}

template <typename Value>
bool
radix_tree<Value>::iterator::operator!=(const iterator &rhs)
{
	return node != rhs.node;
}

template <typename Value>
bool
radix_tree<Value>::iterator::operator==(const iterator &rhs)
{
	return !(*this != rhs);
}

template <typename Value>
template <typename... Args, typename T>
typename std::enable_if<std::is_same<T, inline_string>::value>::type
radix_tree<Value>::iterator::assign(string_view v)
{
	auto pop = pool_base(pmemobj_pool_by_ptr(node.get_leaf()));
	actions acts(pop, g_pool_id);

	auto old_leaf = node.get_leaf();
	tagged_node_ptr *child_slot;

	if (old_leaf->parent)
		child_slot = old_leaf->parent->find_child(node).operator->();
	else
		child_slot = &tree->root;

	acts.free(node.offset());

	tagged_node_ptr new_leaf =
		leaf::make(acts, old_leaf->parent, old_leaf->key(), v);

	acts.set((uint64_t *)child_slot, new_leaf.offset());

	acts.publish();
}

template <typename Value>
template <typename ChildIterator>
typename radix_tree<Value>::tagged_node_ptr
radix_tree<Value>::next(tagged_node_ptr n)
{
	/* It must be dereferenceable. */
	assert(n);

	auto parent = parent_ref(n);

	if (!parent)
		return nullptr; // XXX: return rightmost leaf?

	auto child_slot = parent->template find_child<ChildIterator>(n);

	return next_node(child_slot);
}

template <typename Value>
template <typename ChildIterator>
typename radix_tree<Value>::tagged_node_ptr
radix_tree<Value>::next_node(
	ChildIterator child_slot) // XXX: find a better name
{
	auto parent = child_slot.get_node();

	do {
		++child_slot;
	} while (child_slot != parent->template end<ChildIterator>() &&
		 !(*child_slot));

	/* No more childeren on this level, need to go up. */
	if (!(child_slot != parent->template end<ChildIterator>()))
		return radix_tree::next<ChildIterator>(tagged_node_ptr(parent));

	return next_leaf<ChildIterator>(*child_slot);
}

template <typename Value>
template <typename ChildIterator>
typename radix_tree<Value>::tagged_node_ptr
radix_tree<Value>::next_leaf(typename radix_tree<Value>::tagged_node_ptr n)
{
	if (n.is_leaf())
		return n;

	for (auto it = n->template begin<ChildIterator>();
	     it != n->template end<ChildIterator>(); ++it) {
		if (*it)
			return next_leaf<ChildIterator>(*it);
	}

	/* There must be at least one leaf at the bottom. */
	assert(false);
}

template <typename Value>
template <typename... Args>
persistent_ptr<typename radix_tree<Value>::leaf>
radix_tree<Value>::leaf::make(actions &acts, tagged_node_ptr parent,
			      Args &&... args)
{
	auto ptr = make_internal(acts, std::forward<Args>(args)...);
	ptr->parent = parent;

	return ptr;
}

template <typename Value>
string_view
radix_tree<Value>::leaf::key()
{
	return key_accessor().get();
}

template <typename Value>
template <typename T>
typename std::enable_if<!std::is_same<T, inline_string>::value,
			typename radix_tree<Value>::mapped_type &>::type
radix_tree<Value>::leaf::value()
{
	return data.second;
}

template <typename Value>
template <typename T>
typename std::enable_if<std::is_same<T, inline_string>::value,
			string_view>::type
radix_tree<Value>::leaf::value()
{
	return value_accessor().get();
}

template <typename Value>
template <typename... Args, typename T, typename Enable>
radix_tree<Value>::leaf::leaf(string_view key, Args &&... args)
    : data(std::piecewise_construct,
	   std::forward_as_tuple(key, reinterpret_cast<char *>(this + 1)),
	   std::forward_as_tuple(std::forward<Args>(args)...))
{
}

template <typename Value>
template <typename T, typename Enable>
radix_tree<Value>::leaf::leaf(string_view key, string_view value)
    : data(std::piecewise_construct,
	   std::forward_as_tuple(key, reinterpret_cast<char *>(this + 1)),
	   std::forward_as_tuple(
		   value, reinterpret_cast<char *>(this + 1) + key.size()))
{
}

template <typename Value>
template <typename... Args, typename T>
typename std::enable_if<!std::is_same<T, inline_string>::value,
			persistent_ptr<typename radix_tree<Value>::leaf>>::type
radix_tree<Value>::leaf::make_internal(actions &acts, string_view key,
				       Args &&... args)
{
	return acts.make<leaf>(sizeof(leaf) + key.size(), key,
			       std::forward<Args>(args)...);
}

template <typename Value>
template <typename T>
typename std::enable_if<std::is_same<T, inline_string>::value,
			persistent_ptr<typename radix_tree<Value>::leaf>>::type
radix_tree<Value>::leaf::make_internal(actions &acts, string_view key,
				       string_view value)
{
	return acts.make<leaf>(sizeof(leaf) + key.size() + value.size(), key,
			       value);
}

template <typename Value>
typename radix_tree<Value>::key_type::accessor
radix_tree<Value>::leaf::key_accessor()
{
	auto k_memory = reinterpret_cast<char *>(this + 1);
	return key_type::accessor(k_memory, data.first);
}

template <typename Value>
template <typename T>
typename std::enable_if<std::is_same<T, inline_string>::value,
			typename T::accessor>::type
radix_tree<Value>::leaf::value_accessor()
{
	auto v_memory = reinterpret_cast<char *>(this + 1) + key().size();
	return key_type::accessor(v_memory, data.second);
}

}
}

#endif /* LIBPMEMOBJ_CPP_RADIX_HPP */
