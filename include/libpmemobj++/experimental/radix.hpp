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
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/string_view.hpp>
#include <libpmemobj++/transaction.hpp>

#include <algorithm>
#include <iostream>
#include <string>

#include <libpmemobj++/detail/pair.hpp>

namespace pmem
{

	namespace detail
{

struct bytes_view
{
	template <typename T,
	  typename Enable =
		  typename std::enable_if<std::is_trivial<T>::value>::type>
obj::string_view
operator()(const T &t)
{
	return {reinterpret_cast<const char *>(&t), sizeof(T)};
}

obj::string_view
operator()(obj::string_view v)
{
	return v;
}

// XXX - add (and require in radix) a is_transparent field
};
}

namespace obj
{

namespace experimental
{

template <typename Key, typename Value, typename BytesView = pmem::detail::bytes_view>
class radix_tree {
	struct inline_string_reference;
	template <bool IsConst>
	struct radix_tree_iterator;

public:
	using key_type = Key;
	using mapped_type = Value;
	using value_type = pmem::detail::pair<const key_type, Value>;
	using size_type = std::size_t;
	using reference = typename std::conditional<
		std::is_same<Value, inline_string>::value,
		inline_string_reference, mapped_type &>::type;
	using const_reference = typename std::conditional<
		std::is_same<Value, inline_string>::value,
		const inline_string_reference, const mapped_type &>::type;

	using const_key_reference = typename std::conditional<
		std::is_same<Key, inline_string>::value, string_view,
		const key_type &>::type;

	using iterator = radix_tree_iterator<false>;
	using const_iterator = radix_tree_iterator<true>;
	// XXX: struct const_reference;

	radix_tree();
	~radix_tree();

	template <class... Args>
	std::pair<iterator, bool> try_emplace(const_key_reference k,
					      Args &&... args);
	template <class... Args>
	std::pair<iterator, bool> emplace(Args &&... args);
	iterator erase(iterator pos);
	void erase(iterator first, iterator last);
	size_type erase(const_key_reference k);
	void clear();

	size_type count(const_key_reference k) const;
	template <typename K>
	size_type count(const K& k) const;

	iterator find(const_key_reference k);
	template <typename K>
	iterator find(const K& k);
	const_iterator find(const_key_reference k) const;
	template <typename K>
	const_iterator find(const K& k) const;

	iterator lower_bound(const_key_reference k) const;
	iterator upper_bound(const_key_reference k) const;

	iterator begin();
	iterator end();
	const_iterator begin() const;
	const_iterator end() const;

	uint64_t size() const noexcept;

	template <typename K, typename V>
	friend std::ostream &operator<<(std::ostream &os,
					const radix_tree<K, V> &tree);

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
	p<uint64_t> size_;

	/* helper functions */
	template <typename F, class... Args>
	std::pair<iterator, bool> internal_emplace(const_key_reference k, F &&);
	template <typename K>
	const_iterator internal_find(const K& k) const;

	static tagged_node_ptr &parent_ref(tagged_node_ptr n);
	static bool keys_equal(string_view k1, string_view k2);
	template <typename ChildIterator>
	static const tagged_node_ptr *next_leaf(ChildIterator child_slot);
	template <typename ChildIterator>
	static const tagged_node_ptr &find_leaf(tagged_node_ptr const &n);
	static unsigned slice_index(char k, uint8_t shift);
	static byten_t prefix_diff(string_view lhs, string_view rhs);
	leaf *bottom_leaf(tagged_node_ptr n);
	leaf *descend(string_view key);
	static void print_rec(std::ostream &os, radix_tree::tagged_node_ptr n);
};

template <typename Key, typename Value, typename BytesView>
struct radix_tree<Key, Value, BytesView>::tagged_node_ptr {
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

	radix_tree::node *operator->() const noexcept;

	explicit operator bool() const noexcept;

private:
	template <typename T>
	T get() const noexcept;

	p<uint64_t> off;
};

template <typename Key, typename Value, typename BytesView>
struct radix_tree<Key, Value, BytesView>::leaf {
	using tree_type = radix_tree<Key, Value, BytesView>;

	template <typename... Args>
	static persistent_ptr<leaf> make(tagged_node_ptr parent,
					 Args &&... args);

	template <typename K = Key>
	typename std::enable_if<!std::is_same<K, inline_string>::value,
				const_key_reference>::type
	key();

	template <typename K = Key>
	typename std::enable_if<std::is_same<K, inline_string>::value,
				const_key_reference>::type
	key();

	tagged_node_ptr parent = nullptr;
	value_type data;

private:
	template <typename... Args>
	leaf(Args &&... args);

	/* (T, T) */
	template <typename... Args, typename K = Key, typename T = Value>
	static typename std::enable_if<
		!std::is_same<K, inline_string>::value &&
			!std::is_same<T, inline_string>::value,
		persistent_ptr<leaf>>::type
	make_internal(Args &&... args);

	/* (T, inline_string) */
	template <typename K = Key, typename T = Value>
	static typename std::enable_if<
		!std::is_same<K, inline_string>::value &&
			std::is_same<T, inline_string>::value,
		persistent_ptr<leaf>>::type
	make_internal(K &&k, string_view value);

	/* (T, inline_string) */
	template <typename... Args, typename K = Key, typename T = Value>
	static typename std::enable_if<
		!std::is_same<K, inline_string>::value &&
			std::is_same<T, inline_string>::value,
		persistent_ptr<leaf>>::type
	make_internal(std::piecewise_construct_t, std::tuple<Args...> k_args,
		      std::tuple<string_view> v_arg);

	/* (inline_string, T) */
	template <typename... Args, typename K = Key, typename T = Value>
	static typename std::enable_if<
		std::is_same<K, inline_string>::value &&
			!std::is_same<T, inline_string>::value,
		persistent_ptr<leaf>>::type
	make_internal(string_view key, Args &&... args);

	/* (inline_string, inline_string) */
	template <typename K = Key, typename T = Value>
	static typename std::enable_if<
		std::is_same<K, inline_string>::value &&
			std::is_same<T, inline_string>::value,
		persistent_ptr<leaf>>::type
	make_internal(string_view key, string_view value);
};

/*
 * Internal nodes store SLNODES children ptrs + one leaf ptr.
 * The leaf ptr is used only for nodes for which length of the path from
 * root is a multiple of byte (n->bit == 8 - SLICE).
 *
 * This ptr stores pointer to a leaf
 * which is a prefix of some other (bottom) leaf.
 */
template <typename Key, typename Value, typename BytesView>
struct radix_tree<Key, Value, BytesView>::node {
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

template <typename Key, typename Value, typename BytesView>
struct radix_tree<Key, Value, BytesView>::inline_string_reference {
	operator string_view() const noexcept;

	template <typename... Args>
	inline_string_reference &operator=(string_view rhs);

private:
	tagged_node_ptr *leaf_ = nullptr;

	friend class radix_tree<Key, Value, BytesView>;

	inline_string_reference(tagged_node_ptr *leaf_) noexcept;
	inline_string::reference get() noexcept;
	inline_string::const_reference get() const noexcept;

	template <typename K = Key>
	typename std::enable_if<std::is_same<K, inline_string>::value,
				ptrdiff_t>::type
	value_offset() const noexcept;

	template <typename K = Key>
	typename std::enable_if<!std::is_same<K, inline_string>::value,
				ptrdiff_t>::type
	value_offset() const noexcept;
};

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
struct radix_tree<Key, Value, BytesView>::radix_tree_iterator {
private:
	using node_ptr = typename std::conditional<IsConst, const tagged_node_ptr*, tagged_node_ptr*>::type;
	friend struct radix_tree_iterator<true>;

public:
	using difference_type = std::ptrdiff_t;
	using value_type = radix_tree::value_type;
	using pointer = typename std::conditional<IsConst, const value_type*, value_type*>::type;
	using reference = typename std::conditional<IsConst, pmem::detail::pair<radix_tree::const_key_reference,
				       radix_tree::const_reference>, pmem::detail::pair<radix_tree::const_key_reference,
				       radix_tree::reference>>::type;
	using iterator_category = std::bidirectional_iterator_tag;

	radix_tree_iterator() = default;
	radix_tree_iterator(node_ptr);
	radix_tree_iterator(const radix_tree_iterator &rhs) = default;

	radix_tree_iterator &operator=(const radix_tree_iterator &rhs) = default;

	reference operator*() const;

	template <typename K = Key, typename T = Value>
	typename std::enable_if<!std::is_same<K, inline_string>::value &&
					!std::is_same<T, inline_string>::value,
				pointer>::type
	operator->() const;

	typename radix_tree::const_key_reference key() const;

	template <typename T = Value>
	typename std::enable_if<!std::is_same<T, inline_string>::value,
				radix_tree::reference>::type
	value() const;

	template <typename T = Value>
	typename std::enable_if<std::is_same<T, inline_string>::value,
				radix_tree::reference>::type
	value() const;

	radix_tree_iterator operator++();
	radix_tree_iterator operator--();

	bool operator!=(const radix_tree_iterator &rhs) const;
	bool operator==(const radix_tree_iterator &rhs) const;

private:
	friend class radix_tree;

	node_ptr node = nullptr;
};

template <typename Key, typename Value, typename BytesView>
struct radix_tree<Key, Value, BytesView>::node::forward_iterator {
	using difference_type = std::ptrdiff_t;
	using value_type = tagged_node_ptr;
	using pointer = const value_type *;
	using reference = const value_type &;
	using iterator_category = std::input_iterator_tag;

	forward_iterator(pointer ptr, const node *n);

	forward_iterator operator++();
	forward_iterator operator++(int);

	reference operator*() const;
	pointer operator->() const;

	const node *get_node() const;

	bool operator!=(const forward_iterator &rhs) const;

private:
	pointer ptr;
	const node *n;
};

template <typename Key, typename Value, typename BytesView>
struct radix_tree<Key, Value, BytesView>::node::reverse_iterator {
	using difference_type = std::ptrdiff_t;
	using value_type = tagged_node_ptr;
	using pointer = const value_type *;
	using reference = const value_type &;
	using iterator_category = std::input_iterator_tag;

	reverse_iterator(pointer ptr, const node *n);

	reverse_iterator operator++();
	reverse_iterator operator++(int);

	reference operator*() const;
	pointer operator->() const;

	const node *get_node() const;

	bool operator!=(const reverse_iterator &rhs) const;

private:
	pointer ptr;
	const node *n;
};

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::radix_tree() : root(nullptr), size_(0)
{
	static_assert(sizeof(node) == 256,
		      "Internal node should have size equal to 256 bytes.");
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::~radix_tree()
{
	clear();
}

/**
 * @return number of elements.
 */
template <typename Key, typename Value, typename BytesView>
uint64_t
radix_tree<Key, Value, BytesView>::size() const noexcept
{
	return this->size_;
}

/*
 * Find a bottom (leftmost) leaf in a subtree.
 */
template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::leaf *
radix_tree<Key, Value, BytesView>::bottom_leaf(
	typename radix_tree<Key, Value, BytesView>::tagged_node_ptr n)
{
	for (size_t i = 0; i < SLNODES; i++) {
		tagged_node_ptr m;
		if ((m = n->child[i]))
			return m.is_leaf() ? m.get_leaf() : bottom_leaf(m);
	}

	assert(false);
}

/*
 * Returns reference to n->parent (handles both internal and leaf nodes).
 */
template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::tagged_node_ptr &
radix_tree<Key, Value, BytesView>::parent_ref(tagged_node_ptr n)
{
	if (n.is_leaf())
		return n.get_leaf()->parent;

	return n->parent;
}

/*
 * Descends to the leaf that represents a subtree whose all keys
 * share a prefix at least as long as the one common to the key
 * and that subtree.
 */
template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::leaf *
radix_tree<Key, Value, BytesView>::descend(string_view key)
{
	auto n = root;

	while (!n.is_leaf() && n->byte < key.size()) {
		auto nn = n->child[slice_index(key.data()[n->byte], n->bit)];

		if (nn)
			n = nn;
		else {
			n = bottom_leaf(n);
			break;
		}
	}

	if (!n.is_leaf())
		n = bottom_leaf(n);

	return n.get_leaf();
}

/*
 * Checks for key equality.
 */
template <typename Key, typename Value, typename BytesView>
bool
radix_tree<Key, Value, BytesView>::keys_equal(string_view k1, string_view k2)
{
	return k1.size() == k2.size() && k1.compare(k2) == 0;
}

/*
 * Returns length of common prefix of lhs and rhs.
 */
template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::byten_t
radix_tree<Key, Value, BytesView>::prefix_diff(string_view lhs, string_view rhs)
{
	byten_t diff;
	for (diff = 0; diff < std::min(lhs.size(), rhs.size()); diff++) {
		if (lhs.data()[diff] != rhs.data()[diff])
			return diff;
	}

	return diff;
}

template <typename Key, typename Value, typename BytesView>
template <typename F, class... Args>
std::pair<typename radix_tree<Key, Value, BytesView>::iterator, bool>
radix_tree<Key, Value, BytesView>::internal_emplace(const_key_reference k, F &&make_leaf)
{
	auto key = BytesView{}(k);
	auto pop = pool_base(pmemobj_pool_by_ptr(this));

	if (!root) {
		transaction::run(pop, [&] { root = make_leaf(nullptr); });

		return {iterator(&root), true};
	}

	/*
	 * Need to descend the tree twice: first to find a leaf that
	 * represents a subtree whose all keys share a prefix at least as
	 * long as the one common to the new key and that subtree.
	 */
	auto leaf = descend(key);
	auto diff = prefix_diff(key, BytesView{}(leaf->key()));

	/* Descend into the tree again. */
	auto n = root;
	auto child_slot = &root;
	auto prev = n;

	auto min_key_len = std::min(BytesView{}(leaf->key()).size(), key.size());

	bitn_t sh = 8 - SLICE;
	if (diff < BytesView{}(leaf->key()).size() && diff < key.size()) {
		auto at = static_cast<unsigned char>(
			BytesView{}(leaf->key()).data()[diff] ^
			key.data()[diff]);
		sh = pmem::detail::mssb_index((uint32_t)at) & (bitn_t) ~(SLICE - 1);
	}

	while (n && !n.is_leaf() &&
	       (n->byte < diff ||
		(n->byte == diff &&
		 (n->bit > sh || (n->bit == sh && diff < min_key_len))))) {

		prev = n;
		child_slot =
			&n->child[slice_index(key.data()[n->byte], n->bit)];
		n = *child_slot;
	}

	/*
	 * If the divergence point is at same nib as an existing node, and
	 * the subtree there is empty, just place our leaf there and we're
	 * done.  Obviously this can't happen if SLICE == 1.
	 */
	if (!n) {
		assert(diff < BytesView{}(leaf->key()).size() &&
		       diff < key.size());

		transaction::run(pop, [&] { *child_slot = make_leaf(prev); });

		return {iterator(child_slot), true};
	}

	/* New key is a prefix of the leaf key or they are equal. We need to add
	 * leaf ptr to internal node. */
	if (diff == key.size()) {
		if (n.is_leaf() &&
		    BytesView{}(n.get_leaf()->key()).size() == key.size()) {
			return {iterator(child_slot), false};
		}

		if (!n.is_leaf() && n->byte == key.size() &&
		    n->bit == 8 - SLICE) {
			if (n->leaf)
				return {iterator(&n->leaf), false};

			transaction::run(pop, [&] { n->leaf = make_leaf(n); });

			return {iterator(&n->leaf), true};
		}

		tagged_node_ptr node;
		transaction::run(pop, [&] {
			/* We have to add new node at the edge from parent to n
			 */
			node = make_persistent<radix_tree::node>();
			node->leaf = make_leaf(node);
			node->child[slice_index(
				BytesView{}(leaf->key()).data()[diff], sh)] = n;
			node->parent = parent_ref(n);
			node->byte = diff;
			node->bit = sh;

			parent_ref(n) = node;

			*child_slot = node;
		});

		return {iterator(&node->leaf), true};
	}

	if (diff == BytesView{}(leaf->key()).size()) {
		/* Leaf key is a prefix of the new key. We need to convert leaf
		 * to a node.
		 */
		tagged_node_ptr node;
		transaction::run(pop, [&] {
			/* We have to add new node at the edge from parent to n
			 */
			node = make_persistent<radix_tree::node>();
			node->leaf = n;
			node->child[slice_index(key.data()[diff], sh)] =
				make_leaf(node);
			node->parent = parent_ref(n);
			node->byte = diff;
			node->bit = sh;

			parent_ref(n) = node;

			*child_slot = node;
		});

		return {iterator(&node->child[slice_index(key.data()[diff],
							  sh)]),
			true};
	}

	tagged_node_ptr node;
	transaction::run(pop, [&] {
		/* We have to add new node at the edge from parent to n */
		node = make_persistent<radix_tree::node>();
		node->child[slice_index(BytesView{}(leaf->key()).data()[diff],
					sh)] = n;
		node->child[slice_index(key.data()[diff], sh)] =
			make_leaf(node);
		node->parent = parent_ref(n);
		node->byte = diff;
		node->bit = sh;

		parent_ref(n) = node;

		*child_slot = node;
	});

	return {iterator(&node->child[slice_index(key.data()[diff], sh)]),
		true};
}

template <typename Key, typename Value, typename BytesView>
template <class... Args>
std::pair<typename radix_tree<Key, Value, BytesView>::iterator, bool>
radix_tree<Key, Value, BytesView>::try_emplace(const_key_reference k, Args &&... args)
{
	return internal_emplace(k, [&](tagged_node_ptr parent) {
		size_++;
		return leaf::make(parent, k, std::forward<Args>(args)...);
	});
}

template <typename Key, typename Value, typename BytesView>
template <class... Args>
std::pair<typename radix_tree<Key, Value, BytesView>::iterator, bool>
radix_tree<Key, Value, BytesView>::emplace(Args &&... args)
{
	auto pop = pool_base(pmemobj_pool_by_ptr(this));
	std::pair<iterator, bool> ret;

	transaction::run(pop, [&] {
		auto leaf_ = leaf::make(nullptr, std::forward<Args>(args)...);
		auto make_leaf = [&](tagged_node_ptr parent) {
			leaf_->parent = parent;
			size_++;
			return leaf_;
		};

		ret = internal_emplace(leaf_->key(), make_leaf);

		if (!ret.second)
			delete_persistent<leaf>(leaf_);
	});

	return ret;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::size_type
radix_tree<Key, Value, BytesView>::count(const_key_reference k) const
{
	return internal_find(k) != end() ? 1 : 0;
}

template <typename Key, typename Value, typename BytesView>
template <typename K>
typename radix_tree<Key, Value, BytesView>::size_type
radix_tree<Key, Value, BytesView>::count(const K& k) const
{
	return internal_find(k) != end() ? 1 : 0;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::iterator
radix_tree<Key, Value, BytesView>::find(const_key_reference k)
{
	return iterator(const_cast<typename iterator::node_ptr>(internal_find(k).node));
}

template <typename Key, typename Value, typename BytesView>
template <typename K>
typename radix_tree<Key, Value, BytesView>::iterator
radix_tree<Key, Value, BytesView>::find(const K& k)
{
	return iterator(const_cast<typename iterator::node_ptr>(internal_find(k).node));
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::const_iterator
radix_tree<Key, Value, BytesView>::find(const_key_reference k) const
{
	return internal_find(k);
}

template <typename Key, typename Value, typename BytesView>
template <typename K>
typename radix_tree<Key, Value, BytesView>::const_iterator
radix_tree<Key, Value, BytesView>::find(const K& k) const
{
	return internal_find(k);
}


template <typename Key, typename Value, typename BytesView>
template <typename K>
typename radix_tree<Key, Value, BytesView>::const_iterator
radix_tree<Key, Value, BytesView>::internal_find(const K& k) const
{
	auto key = BytesView{}(k);

	auto n = root;
	auto child_slot = &root;
	while (n && !n.is_leaf()) {
		if (n->byte == key.size() && n->bit == 8 - SLICE)
			child_slot = &n->leaf;
		else if (n->byte > key.size())
			return end();
		else
			child_slot = &n->child[slice_index(key.data()[n->byte],
							   n->bit)];

		n = *child_slot;
	}

	if (!n)
		return end();

	if (!keys_equal(key, BytesView{}(n.get_leaf()->key())))
		return end();

	return const_iterator(child_slot);
}

template <typename Key, typename Value, typename BytesView>
void
radix_tree<Key, Value, BytesView>::clear()
{
	if (size() != 0)
		erase(begin(), end());
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::iterator
radix_tree<Key, Value, BytesView>::erase(iterator pos)
{
	auto pop = pool_base(pmemobj_pool_by_ptr(this));

	transaction::run(pop, [&] {
		auto *leaf = pos.node;
		auto parent = leaf->get_leaf()->parent;

		delete_persistent<radix_tree::leaf>(leaf->get_leaf());

		size_--;

		/* was root */
		if (!parent) {
			root = nullptr;
			pos = begin();
			return;
		}

		++pos;
		*leaf = nullptr;

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

		auto *child_slot =
			parent ? const_cast<tagged_node_ptr*>(parent->find_child(n).operator->()) : &root;
		*child_slot = only_child;

		/* If iterator now points to only_child it mus be updated
		 * (otherwise it would have reference to freed node). */
		if (pos.node && *pos.node == only_child)
			pos.node = child_slot;

		delete_persistent<radix_tree::node>(n.get_node());
	});

	return pos;
}

template <typename Key, typename Value, typename BytesView>
void
radix_tree<Key, Value, BytesView>::erase(iterator first, iterator last)
{
	auto pop = pool_base(pmemobj_pool_by_ptr(this));

	transaction::run(pop, [&] {
		while (first != last)
			first = erase(first);
	});
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::size_type
radix_tree<Key, Value, BytesView>::erase(const_key_reference k)
{
	auto it = find(k);

	if (it == end())
		return 0;

	erase(it);

	return 1;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::iterator
radix_tree<Key, Value, BytesView>::lower_bound(const_key_reference k) const
{
	auto key = BytesView{}(k);

	auto n = root;
	decltype(n) prev;
	decltype(n) *child_slot = nullptr;

	if (!root)
		return end();

	while (n && !n.is_leaf()) {
		prev = n;

		if (n->byte == key.size() && n->bit == 8 - SLICE)
			child_slot = &n->leaf;
		else if (n->byte > key.size())
			return iterator(
				&find_leaf<typename node::forward_iterator>(n));
		else
			child_slot = &n->child[slice_index(key.data()[n->byte],
							   n->bit)];

		n = *child_slot;
	}

	if (!n) {
		auto child_it = typename node::forward_iterator(
			child_slot, prev.get_node());
		return iterator(next_leaf(child_it));
	}

	assert(n.is_leaf());

	if (n.get_leaf()->key().compare(key) >= 0)
		return iterator(child_slot);

	return ++iterator(child_slot);
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::iterator
radix_tree<Key, Value, BytesView>::upper_bound(const_key_reference k) const
{
	auto key = BytesView{}(k);
	auto it = lower_bound(key);

	// XXX: optimize -> it's 2 comparisons
	if (keys_equal(it.key(), key))
		return ++it;

	return it;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::iterator
radix_tree<Key, Value, BytesView>::begin()
{
	auto const_begin = const_cast<const radix_tree*>(this)->begin();
	return iterator(const_cast<typename iterator::node_ptr>(const_begin.node));
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::iterator
radix_tree<Key, Value, BytesView>::end()
{
	auto const_end = const_cast<const radix_tree*>(this)->end();
	return iterator(const_cast<typename iterator::node_ptr>(const_end.node));
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::const_iterator
radix_tree<Key, Value, BytesView>::begin() const
{
	if (!root)
		return const_iterator(nullptr);

	return const_iterator(
		&radix_tree::find_leaf<
			typename radix_tree::node::forward_iterator>(root));
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::const_iterator
radix_tree<Key, Value, BytesView>::end() const
{
	return const_iterator(nullptr);
}

template <typename Key, typename Value, typename BytesView>
void
radix_tree<Key, Value, BytesView>::print_rec(std::ostream &os,
				  radix_tree::tagged_node_ptr n)
{
	if (!n.is_leaf()) {
		auto parent = n->parent ? n->parent.get_node() : nullptr;
		os << (uint64_t)n.get_node() << "->" << (uint64_t)parent
		   << " [label=\""
		   << "parent"
		   << "\"]" << std::endl;

		if (n->leaf) {
			os << (uint64_t)n.get_node() << "->"
			   << (uint64_t)n->leaf.get_leaf() << " [label=\""
			   << n->leaf.get_leaf()->key().data() << "\"]"
			   << std::endl;
			print_rec(os, n->leaf);
		}

		for (int i = 0; i < (int)SLNODES; i++) {
			if (n->child[i]) {
				auto label = n->child[i].is_leaf()
					? n->child[i].get_leaf()->key().data()
					: "-";

				auto ch = n->child[i].is_leaf()
					? (uint64_t)n->child[i].get_leaf()
					: (uint64_t)n->child[i].get_node();

				os << (uint64_t)n.get_node() << " -> "
				   << (uint64_t)ch << " [label=\"" << label
				   << "\"]" << std::endl;
				print_rec(os, n->child[i]);
			}
		}
	} else {
		auto parent = n.get_leaf()->parent
			? n.get_leaf()->parent.get_node()
			: nullptr;

		os << (uint64_t)n.get_leaf() << "->" << (uint64_t)parent
		   << " [label=\""
		   << "parent"
		   << "\"]" << std::endl;

		if (parent && n == parent->leaf) {
			os << "{rank=same;" << (uint64_t)parent << ";"
			   << (uint64_t)n.get_leaf() << "}" << std::endl;
		}
	}
}

/**
 * Prints tree in DOT format. Used for debugging.
 */
template <typename K, typename V>
std::ostream &
operator<<(std::ostream &os, const radix_tree<K, V> &tree)
{
	os << "digraph Radix {" << std::endl;

	if (tree.root)
		radix_tree<K, V>::print_rec(os, tree.root);

	os << "}" << std::endl;

	return os;
}

/*
 * internal: slice_index -- return index of child at the given nib
 */
template <typename Key, typename Value, typename BytesView>
unsigned
radix_tree<Key, Value, BytesView>::slice_index(char b, uint8_t bit)
{
	return static_cast<unsigned>(b >> bit) & NIB;
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::tagged_node_ptr::tagged_node_ptr(std::nullptr_t)
{
	this->off = 0;
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::tagged_node_ptr::tagged_node_ptr()
{
	this->off = 0;
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::tagged_node_ptr::tagged_node_ptr(
	const tagged_node_ptr &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		this->off =
			rhs.get<uint64_t>() - reinterpret_cast<uint64_t>(this);
		off |= unsigned(rhs.is_leaf());
	}
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::tagged_node_ptr &
radix_tree<Key, Value, BytesView>::tagged_node_ptr::operator=(const tagged_node_ptr &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		this->off =
			rhs.get<uint64_t>() - reinterpret_cast<uint64_t>(this);
		off |= unsigned(rhs.is_leaf());
	}

	return *this;
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::tagged_node_ptr::tagged_node_ptr(
	const persistent_ptr<leaf> &ptr)
{
	if (!ptr) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(ptr.get()) -
			reinterpret_cast<uint64_t>(this);
		off |= 1U;
	}
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::tagged_node_ptr::tagged_node_ptr(
	const persistent_ptr<node> &ptr)
{
	if (!ptr) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(ptr.get()) -
			reinterpret_cast<uint64_t>(this);
	}
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::tagged_node_ptr &
	radix_tree<Key, Value, BytesView>::tagged_node_ptr::operator=(std::nullptr_t)
{
	this->off = 0;

	return *this;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::tagged_node_ptr &
radix_tree<Key, Value, BytesView>::tagged_node_ptr::operator=(
	const persistent_ptr<leaf> &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(rhs.get()) -
			reinterpret_cast<uint64_t>(this);
		off |= 1U;
	}

	return *this;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::tagged_node_ptr &
radix_tree<Key, Value, BytesView>::tagged_node_ptr::operator=(
	const persistent_ptr<node> &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(rhs.get()) -
			reinterpret_cast<uint64_t>(this);
	}

	return *this;
}

template <typename Key, typename Value, typename BytesView>
bool
radix_tree<Key, Value, BytesView>::tagged_node_ptr::operator==(
	const radix_tree::tagged_node_ptr &rhs) const
{
	return get<uint64_t>() == rhs.get<uint64_t>() || (!*this && !rhs);
}

template <typename Key, typename Value, typename BytesView>
bool
radix_tree<Key, Value, BytesView>::tagged_node_ptr::operator!=(
	const radix_tree::tagged_node_ptr &rhs) const
{
	return !(*this == rhs);
}

template <typename Key, typename Value, typename BytesView>
bool
radix_tree<Key, Value, BytesView>::tagged_node_ptr::is_leaf() const
{
	return off & 1U;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::leaf *
radix_tree<Key, Value, BytesView>::tagged_node_ptr::get_leaf() const
{
	assert(is_leaf());
	return get<radix_tree::leaf *>();
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node *
radix_tree<Key, Value, BytesView>::tagged_node_ptr::get_node() const
{
	assert(!is_leaf());
	return get<radix_tree::node *>();
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::tagged_node_ptr::operator bool() const noexcept
{
	return (off & ~uint64_t(1)) != 0;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node *
	radix_tree<Key, Value, BytesView>::tagged_node_ptr::operator->() const noexcept
{
	return get_node();
}

template <typename Key, typename Value, typename BytesView>
template <typename T>
T
radix_tree<Key, Value, BytesView>::tagged_node_ptr::get() const noexcept
{
	auto s = reinterpret_cast<T>(reinterpret_cast<uint64_t>(this) +
				     (off & ~uint64_t(1)));
	return s;
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::node::forward_iterator::forward_iterator(
	pointer ptr, const node *n)
    : ptr(ptr), n(n)
{
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node::forward_iterator
radix_tree<Key, Value, BytesView>::node::forward_iterator::operator++()
{
	if (ptr == &n->leaf)
		ptr = &n->child[0];
	else
		ptr++;

	return *this;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node::forward_iterator
radix_tree<Key, Value, BytesView>::node::forward_iterator::operator++(int)
{
	forward_iterator tmp(ptr, n);
	operator++();
	return tmp;
}
template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node::forward_iterator::reference
	radix_tree<Key, Value, BytesView>::node::forward_iterator::operator*() const
{
	return *ptr;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node::forward_iterator::pointer
	radix_tree<Key, Value, BytesView>::node::forward_iterator::operator->() const
{
	return ptr;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node const *
radix_tree<Key, Value, BytesView>::node::forward_iterator::get_node() const
{
	return n;
}

template <typename Key, typename Value, typename BytesView>
bool
radix_tree<Key, Value, BytesView>::node::forward_iterator::operator!=(
	const forward_iterator &rhs) const
{
	return ptr != rhs.ptr;
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::node::reverse_iterator::reverse_iterator(
	pointer ptr, const node *n)
    : ptr(ptr), n(n)
{
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node::reverse_iterator
radix_tree<Key, Value, BytesView>::node::reverse_iterator::operator++()
{
	if (ptr == &n->child[0])
		ptr = &n->leaf;
	else
		ptr--;

	return *this;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node::reverse_iterator
radix_tree<Key, Value, BytesView>::node::reverse_iterator::operator++(int)
{
	reverse_iterator tmp(ptr, n);
	operator++();
	return tmp;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node::reverse_iterator::reference
	radix_tree<Key, Value, BytesView>::node::reverse_iterator::operator*() const
{
	return *ptr;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node::reverse_iterator::pointer
	radix_tree<Key, Value, BytesView>::node::reverse_iterator::operator->() const
{
	return ptr;
}

template <typename Key, typename Value, typename BytesView>
bool
radix_tree<Key, Value, BytesView>::node::reverse_iterator::operator!=(
	const reverse_iterator &rhs) const
{
	return ptr != rhs.ptr;
}

template <typename Key, typename Value, typename BytesView>
typename radix_tree<Key, Value, BytesView>::node const*
radix_tree<Key, Value, BytesView>::node::reverse_iterator::get_node() const
{
	return n;
}

template <typename Key, typename Value, typename BytesView>
template <typename Iterator>
typename std::enable_if<
	std::is_same<
		Iterator,
		typename radix_tree<Key, Value, BytesView>::node::forward_iterator>::value,
	Iterator>::type
radix_tree<Key, Value, BytesView>::node::begin()
{
	return Iterator(&leaf, this);
}

template <typename Key, typename Value, typename BytesView>
template <typename Iterator>
typename std::enable_if<
	std::is_same<
		Iterator,
		typename radix_tree<Key, Value, BytesView>::node::forward_iterator>::value,
	Iterator>::type
radix_tree<Key, Value, BytesView>::node::end()
{
	return Iterator(&child[SLNODES], this);
}

template <typename Key, typename Value, typename BytesView>
template <typename Iterator>
typename std::enable_if<
	std::is_same<
		Iterator,
		typename radix_tree<Key, Value, BytesView>::node::reverse_iterator>::value,
	Iterator>::type
radix_tree<Key, Value, BytesView>::node::begin()
{
	return Iterator(&child[SLNODES - 1], this);
}

template <typename Key, typename Value, typename BytesView>
template <typename Iterator>
typename std::enable_if<
	std::is_same<
		Iterator,
		typename radix_tree<Key, Value, BytesView>::node::reverse_iterator>::value,
	Iterator>::type
radix_tree<Key, Value, BytesView>::node::end()
{
	return Iterator(&leaf - 1, this);
}

template <typename Key, typename Value, typename BytesView>
template <typename Iterator>
Iterator
radix_tree<Key, Value, BytesView>::node::find_child(
	radix_tree<Key, Value, BytesView>::tagged_node_ptr n)
{
	return std::find(begin<Iterator>(), end<Iterator>(), n);
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::inline_string_reference::inline_string_reference(
	tagged_node_ptr *leaf_) noexcept
    : leaf_(leaf_)
{
}

template <typename Key, typename Value, typename BytesView>
radix_tree<Key, Value, BytesView>::inline_string_reference::operator string_view() const
	noexcept
{
	return get();
}

/* Returns where value is located (how many bytes after the leaf). */
template <typename Key, typename Value, typename BytesView>
template <typename K>
typename std::enable_if<std::is_same<K, inline_string>::value, ptrdiff_t>::type
radix_tree<Key, Value, BytesView>::inline_string_reference::value_offset() const noexcept
{
	return static_cast<ptrdiff_t>(leaf_->get_leaf()->key().size());
}

/* Returns where value is located (how many bytes after the leaf). */
template <typename Key, typename Value, typename BytesView>
template <typename K>
typename std::enable_if<!std::is_same<K, inline_string>::value, ptrdiff_t>::type
radix_tree<Key, Value, BytesView>::inline_string_reference::value_offset() const noexcept
{
	return 0;
}

template <typename Key, typename Value, typename BytesView>
inline_string::reference
radix_tree<Key, Value, BytesView>::inline_string_reference::get() noexcept
{
	return inline_string::reference(
		reinterpret_cast<char *>(leaf_->get_leaf() + 1) +
			value_offset(),
		leaf_->get_leaf()->data.second);
}

template <typename Key, typename Value, typename BytesView>
inline_string::const_reference
radix_tree<Key, Value, BytesView>::inline_string_reference::get() const noexcept
{
	return inline_string::const_reference(
		reinterpret_cast<const char *>(leaf_->get_leaf() + 1) +
			value_offset(),
		leaf_->get_leaf()->data.second);
}

/*
 * Handles assignemnt to inline_string. If there is enough capacity
 * old content is overwritten (with a help of undo log). Otherwise
 * a new leaf is allocated and the old one is freed.
 */
template <typename Key, typename Value, typename BytesView>
template <typename... Args>
typename radix_tree<Key, Value, BytesView>::inline_string_reference &
radix_tree<Key, Value, BytesView>::inline_string_reference::operator=(string_view rhs)
{
	auto capacity =
		pmemobj_alloc_usable_size(pmemobj_oid(leaf_->get_leaf())) -
		sizeof(leaf) - static_cast<size_t>(value_offset());

	if (rhs.size() <= capacity) {
		get().assign(rhs);
	} else {
		auto pop = pool_base(pmemobj_pool_by_ptr(leaf_->get_leaf()));
		auto old_leaf = leaf_->get_leaf();

		transaction::run(pop, [&] {
			*leaf_ = leaf::make(old_leaf->parent, old_leaf->key(),
					    rhs);
			delete_persistent<typename radix_tree::leaf>(old_leaf);
		});
	}

	return *this;
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::radix_tree_iterator(
	node_ptr node)
    : node(node)
{
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
typename radix_tree<Key, Value, BytesView>::template radix_tree_iterator<IsConst>::reference
	radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::operator*() const
{
	return {key(), value()};
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
template <typename K, typename T>
typename std::enable_if<
	!std::is_same<K, inline_string>::value &&
		!std::is_same<T, inline_string>::value,
	typename radix_tree<Key, Value, BytesView>::template radix_tree_iterator<IsConst>::pointer>::type
	radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::operator->() const
{
	return &node->get_leaf()->data;
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
typename radix_tree<Key, Value, BytesView>::const_key_reference
radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::key() const
{
	return node->get_leaf()->key();
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
template <typename T>
typename std::enable_if<!std::is_same<T, inline_string>::value,
			typename radix_tree<Key, Value, BytesView>::reference>::type
radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::value() const
{
	return node->get_leaf()->data.second;
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
template <typename T>
typename std::enable_if<std::is_same<T, inline_string>::value,
			typename radix_tree<Key, Value, BytesView>::reference>::type
radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::value() const
{
	return radix_tree::reference(node);
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
typename radix_tree<Key, Value, BytesView>::template radix_tree_iterator<IsConst>
radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::operator++()
{
	assert(node);

	/* node is root, there is no other leaf in the tree */
	if (!node->get_leaf()->parent)
		node = nullptr;
	else
		node = next_leaf(typename node::forward_iterator(
			node, node->get_leaf()->parent.get_node()));

	return *this;
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
typename radix_tree<Key, Value, BytesView>::template radix_tree_iterator<IsConst>
radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::operator--()
{
	assert(node);

	/* node is root, there is no other leaf in the tree */
	if (!node->get_leaf()->parent)
		node = nullptr;
	else
		node = next_leaf(typename node::reverse_iterator(
			node, node->get_leaf()->parent.get_node()));

	return *this;
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
bool
radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::operator!=(const radix_tree_iterator<IsConst> &rhs) const
{
	return node != rhs.node;
}

template <typename Key, typename Value, typename BytesView>
template <bool IsConst>
bool
radix_tree<Key, Value, BytesView>::radix_tree_iterator<IsConst>::operator==(const radix_tree_iterator<IsConst> &rhs) const
{
	return !(*this != rhs);
}

/*
 * Returns next leaf (either with smaller or larger key, depending on
 * ChildIterator type). This function might need to traverse the
 * tree upwards.
 */
template <typename Key, typename Value, typename BytesView>
template <typename ChildIterator>
typename radix_tree<Key, Value, BytesView>::tagged_node_ptr const *
radix_tree<Key, Value, BytesView>::next_leaf(ChildIterator child_slot)
{
	auto parent = child_slot.get_node();

	do {
		++child_slot;
	} while (child_slot != parent->template end<ChildIterator>() &&
		 !(*child_slot));

	/* No more childeren on this level, need to go up. */
	if (!(child_slot != parent->template end<ChildIterator>())) {
		auto p = parent->parent;
		if (!p)
			return nullptr;

		return next_leaf(p->template find_child<ChildIterator>(
			tagged_node_ptr(parent)));
	}

	return &find_leaf<ChildIterator>(*child_slot);
}

/*
 * Returns smallest (or biggest, depending on ChildIterator) leaf
 * in a subtree.
 */
template <typename Key, typename Value, typename BytesView>
template <typename ChildIterator>
typename radix_tree<Key, Value, BytesView>::tagged_node_ptr const &
radix_tree<Key, Value, BytesView>::find_leaf(
	typename radix_tree<Key, Value, BytesView>::tagged_node_ptr const &n)
{
	if (n.is_leaf())
		return n;

	for (auto it = n->template begin<ChildIterator>();
	     it != n->template end<ChildIterator>(); ++it) {
		if (*it)
			return find_leaf<ChildIterator>(*it);
	}

	/* There must be at least one leaf at the bottom. */
	assert(false);
}

template <typename Key, typename Value, typename BytesView>
template <typename... Args>
persistent_ptr<typename radix_tree<Key, Value, BytesView>::leaf>
radix_tree<Key, Value, BytesView>::leaf::make(tagged_node_ptr parent, Args &&... args)
{
	auto ptr = make_internal(std::forward<Args>(args)...);
	ptr->parent = parent;

	return ptr;
}

template <typename Key, typename Value, typename BytesView>
template <typename K>
typename std::enable_if<
	std::is_same<K, inline_string>::value,
	typename radix_tree<Key, Value, BytesView>::const_key_reference>::type
radix_tree<Key, Value, BytesView>::leaf::key()
{
	auto k_memory = reinterpret_cast<char *>(this + 1);
	return typename key_type::const_reference(k_memory, data.first);
}

template <typename Key, typename Value, typename BytesView>
template <typename K>
typename std::enable_if<
	!std::is_same<K, inline_string>::value,
	typename radix_tree<Key, Value, BytesView>::const_key_reference>::type
radix_tree<Key, Value, BytesView>::leaf::key()
{
	return data.first;
}

template <typename Key, typename Value, typename BytesView>
template <typename... Args>
radix_tree<Key, Value, BytesView>::leaf::leaf(Args &&... args)
    : data(std::forward<Args>(args)...)
{
}

template <typename Key, typename Value, typename BytesView>
template <typename... Args, typename K, typename T>
typename std::enable_if<
	!std::is_same<K, inline_string>::value &&
		!std::is_same<T, inline_string>::value,
	persistent_ptr<typename radix_tree<Key, Value, BytesView>::leaf>>::type
radix_tree<Key, Value, BytesView>::leaf::make_internal(Args &&... args)
{
	standard_alloc_policy<void> a;
	auto ptr = static_cast<persistent_ptr<leaf>>(a.allocate(sizeof(leaf)));

	new (ptr.get()) leaf(std::forward<Args>(args)...);

	return ptr;
}

template <typename Key, typename Value, typename BytesView>
template <typename K, typename T>
typename std::enable_if<
	!std::is_same<K, inline_string>::value &&
		std::is_same<T, inline_string>::value,
	persistent_ptr<typename radix_tree<Key, Value, BytesView>::leaf>>::type
radix_tree<Key, Value, BytesView>::leaf::make_internal(K &&k, string_view value)
{
	standard_alloc_policy<void> a;
	auto ptr = static_cast<persistent_ptr<leaf>>(
		a.allocate(sizeof(leaf) + value.size()));

	new (ptr.get())
		leaf(std::piecewise_construct,
		     std::forward_as_tuple(std::forward<K>(k)),
		     std::forward_as_tuple(
			     value, reinterpret_cast<char *>(ptr.get() + 1)));

	return ptr;
}

template <typename Key, typename Value, typename BytesView>
template <typename... Args, typename K, typename T>
typename std::enable_if<
	!std::is_same<K, inline_string>::value &&
		std::is_same<T, inline_string>::value,
	persistent_ptr<typename radix_tree<Key, Value, BytesView>::leaf>>::type
radix_tree<Key, Value, BytesView>::leaf::make_internal(std::piecewise_construct_t t,
					    std::tuple<Args...> k_args,
					    std::tuple<string_view> v_arg)
{
	standard_alloc_policy<void> a;
	auto ptr = static_cast<persistent_ptr<leaf>>(
		a.allocate(sizeof(leaf) + std::get<0>(v_arg).size()));

	new (ptr.get()) leaf(
		t, k_args,
		std::forward_as_tuple(std::get<0>(v_arg),
				      reinterpret_cast<char *>(ptr.get() + 1)));

	return ptr;
}

template <typename Key, typename Value, typename BytesView>
template <typename... Args, typename K, typename T>
typename std::enable_if<
	std::is_same<K, inline_string>::value &&
		!std::is_same<T, inline_string>::value,
	persistent_ptr<typename radix_tree<Key, Value, BytesView>::leaf>>::type
radix_tree<Key, Value, BytesView>::leaf::make_internal(string_view key, Args &&... args)
{
	standard_alloc_policy<void> a;
	auto ptr = static_cast<persistent_ptr<leaf>>(
		a.allocate(sizeof(leaf) + key.size()));

	new (ptr.get())
		leaf(std::piecewise_construct,
		     std::forward_as_tuple(
			     key, reinterpret_cast<char *>(ptr.get() + 1)),
		     std::forward_as_tuple(std::forward<Args>(args)...));

	return ptr;
}

template <typename Key, typename Value, typename BytesView>
template <typename K, typename T>
typename std::enable_if<
	std::is_same<K, inline_string>::value &&
		std::is_same<T, inline_string>::value,
	persistent_ptr<typename radix_tree<Key, Value, BytesView>::leaf>>::type
radix_tree<Key, Value, BytesView>::leaf::make_internal(string_view key, string_view value)
{
	standard_alloc_policy<void> a;
	auto ptr = static_cast<persistent_ptr<leaf>>(
		a.allocate(sizeof(leaf) + key.size() + value.size()));

	new (ptr.get()) leaf(
		std::piecewise_construct,
		std::forward_as_tuple(key,
				      reinterpret_cast<char *>(ptr.get() + 1)),
		std::forward_as_tuple(value,
				      reinterpret_cast<char *>(ptr.get() + 1) +
					      key.size()));

	return ptr;
}

}
}
}

#endif /* LIBPMEMOBJ_CPP_RADIX_HPP */
