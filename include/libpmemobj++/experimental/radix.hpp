#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/shared_mutex.hpp>

#include <libpmemobj++/container/string.hpp>

#include <atomic>
#include <shared_mutex>

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/slice.hpp>
#include <libpmemobj++/transaction.hpp>

#include <libpmemobj/action_base.h>

#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include <libpmemobj++/detail/pair.hpp>

namespace pmem
{

namespace detail
{

class inline_string {
public:
	inline_string(std::string_view str, char *data) : size_(str.size())
	{
		std::memcpy(data, str.data(), size_);
	}

	obj::slice<char *>
	data(char *data_) const
	{
		return {data_, data_ + size_};
	}

private:
	const uint64_t size_;
};

}

namespace obj
{

template <class T, template <class...> class Template>
struct is_specialization : std::false_type {
};

template <template <class...> class Template, class... Args>
struct is_specialization<Template<Args...>, Template> : std::true_type {
};

// XXX - reimplement this pair
template <typename Value>
struct inline_pair : public detail::pair<detail::inline_string, Value> {
	using detail::pair<detail::inline_string, Value>::pair;
	using detail::pair<detail::inline_string, Value>::first;
	using detail::pair<detail::inline_string, Value>::second;

	std::string_view
	key()
	{
		auto r = first.data(reinterpret_cast<char *>(this + 1));
		return {r.begin(), r.size()};
	}

	Value &
	value()
	{
		return second;
	}
};

// template <>
// struct inline_pair<detail::inline_string> : public
// detail::pair<detail::inline_string, detail::inline_string>
// {
// 	using detail::pair<detail::inline_string, detail::inline_string>::pair;
// 	using detail::pair<detail::inline_string, detail::inline_string>::first;
// 	using detail::pair<detail::inline_string,
// detail::inline_string>::second;

// 	std::string_view key() {
// 		auto r = first.data(reinterpret_cast<char*>(this + 1));
// 		return {r.begin(), r.size()};
// 	}

// 	obj::slice<char*> value() {
// 		return second.data(reinterpret_cast<char*>(this + 1) +
// key().size());
// 	}
// };

template <typename Value, typename Pointer>
struct tree_leaf {
	template <typename... Args>
	tree_leaf(Args&&... args): data(std::forward<Args>(args)...)
	{}

	Pointer parent = nullptr;
	Value data;
};

template <typename T, typename... Args>
typename std::enable_if<is_specialization<T, tree_leaf>::value,
			persistent_ptr<T>>::type
make_persistent(std::string_view k, Args &&... args)
{
	if (pmemobj_tx_stage() != TX_STAGE_WORK)
		throw pmem::transaction_scope_error(
			"refusing to allocate memory outside of transaction scope");

	persistent_ptr<T> ptr = pmemobj_tx_xalloc(sizeof(T) + k.size(),
						  detail::type_num<T>(), 0);

	if (ptr == nullptr) {
		if (errno == ENOMEM)
			throw pmem::transaction_out_of_memory(
				"Failed to allocate persistent memory object")
				.with_pmemobj_errormsg();
		else
			throw pmem::transaction_alloc_error(
				"Failed to allocate persistent memory object")
				.with_pmemobj_errormsg();
	}

	auto k_dest = reinterpret_cast<char *>(ptr.get()) + sizeof(T);

	new (ptr.get())
		T(std::piecewise_construct, std::forward_as_tuple(k, k_dest),
		  std::forward_as_tuple(std::forward<Args>(args)...));

	return ptr;
}

// template <typename... Args>
// persistent_ptr<inline_pair<detail::inline_string>>
// make_persistent_inline<inline_pair<detail::inline_string>>(std::string_view
// k, std::string_view v)
// {
// 	if (pmemobj_tx_stage() != TX_STAGE_WORK)
// 		throw pmem::transaction_scope_error(
// 			"refusing to allocate memory outside of transaction
// scope");

// 	persistent_ptr<inline_pair<detail::inline_string>> ptr =
// pmemobj_tx_xalloc(sizeof(inline_pair<detail::inline_string>) + k.size() +
// v.size(), 						  detail::type_num<inline_pair<detail::inline_string>>(), 0);

// 	if (ptr == nullptr) {
// 		if (errno == ENOMEM)
// 			throw pmem::transaction_out_of_memory(
// 				"Failed to allocate persistent memory object")
// 				.with_pmemobj_errormsg();
// 		else
// 			throw pmem::transaction_alloc_error(
// 				"Failed to allocate persistent memory object")
// 				.with_pmemobj_errormsg();
// 	}

// 	auto k_dest = reinterpret_cast<char *>(ptr.get()) +
// sizeof(inline_pair<detail::inline_string>); 	auto v_dest = k_dest + k.size();
// 	detail::create<inline_pair<detail::inline_string>, Args...>(
// 		ptr.get(), std::piecewise_construct,
// 		std::forward_as_tuple(k, k_dest),
// 		std::forward_as_tuple(v, v_dest));
// }

static constexpr std::size_t SLICE = 4;
static constexpr std::size_t NIB = ((1ULL << SLICE) - 1);
static constexpr std::size_t SLNODES = (1 << SLICE);

using byten_t = uint32_t;
using bitn_t = uint8_t;

using string_view = std::string_view;

/**
 * Based on: https://github.com/pmem/pmdk/blob/master/src/libpmemobj/critnib.h
 */
template <typename Value>
class radix_tree {
private:
	struct tagged_node_ptr;

	using leaf = tree_leaf<inline_pair<Value>, tagged_node_ptr>;
	struct node;

	struct tagged_node_ptr {
		tagged_node_ptr();
		tagged_node_ptr(const tagged_node_ptr &rhs);
		tagged_node_ptr(std::nullptr_t);

		tagged_node_ptr(const obj::persistent_ptr<leaf> &ptr);
		tagged_node_ptr(const obj::persistent_ptr<node> &ptr);

		tagged_node_ptr &operator=(const tagged_node_ptr &rhs);
		tagged_node_ptr &operator=(std::nullptr_t);
		tagged_node_ptr &
		operator=(const obj::persistent_ptr<leaf> &rhs);
		tagged_node_ptr &
		operator=(const obj::persistent_ptr<node> &rhs);

		bool operator==(const tagged_node_ptr &rhs) const;
		bool operator!=(const tagged_node_ptr &rhs) const;

		bool is_leaf() const;

		radix_tree::leaf *get_leaf() const;
		radix_tree::node *get_node() const;

		radix_tree::node *operator->() const noexcept;

		explicit operator bool() const noexcept;

	private:
		template <typename T>
		T
		get() const noexcept
		{
			auto s = reinterpret_cast<T>(
				reinterpret_cast<uint64_t>(this) +
				(off & ~uint64_t(1)));
			return s;
		}

		obj::p<uint64_t> off;
	};

	/*
	 * Internal nodes store SLNODES children ptrs + one leaf ptr.
	 * The leaf ptr is used only for nodes for which length of the path from
	 * root is a multiple of byte (n->bit == 8 - SLICE).
	 *
	 * This ptr stores pointer to a leaf
	 * which is a prefix of some other leaf.
	 */
	struct node {
		tagged_node_ptr leaf; // -> ptr<leaf>
		tagged_node_ptr child[SLNODES];
		tagged_node_ptr parent;
		byten_t byte;
		bitn_t bit;

		tagged_node_ptr *
		find_child(tagged_node_ptr ptr)
		{
			if (leaf == ptr)
				return &leaf;

			return std::find(child, child + SLNODES, ptr);

			// return std::find(&leaf, child + SLNODES, ptr);
		}

		// uint8_t padding[256 - sizeof(mtx) - sizeof(child) -
		// sizeof(byte) - 		sizeof(bit)];
	};

	// static_assert(sizeof(node) == 256, "Wrong node size");

public:
	using key_type = detail::inline_string;
	using mapped_type = Value;

	// XXX key-type
	using value_type = inline_pair<Value>;

	// using user_value_type =
	// std::pair<decltype(std::declval<inline_pair<Value>>().key()),
	// decltype(std::declval<inline_pair<Value>>().value())>;
	// XXX
	// using iterator = user_value_type *;

	struct iterator {
		iterator(std::nullptr_t): node(nullptr)
		{

		}

		iterator(tagged_node_ptr node) : node(node)
		{
		}

		value_type *operator->()
		{
			return &(**this);
		}

		value_type &operator*()
		{
			if (node.is_leaf())
				return node.get_leaf()->data;

			return node->leaf.get_leaf()->data;
		}

		iterator
		operator++()
		{
			node = next_node(node);
			return *this;
		}

	private:
		tagged_node_ptr node;

		tagged_node_ptr
		next_node(tagged_node_ptr n)
		{
			auto parent = node.is_leaf() ? node.get_leaf()->parent
						     : node->parent;
			auto *child_slot = parent->find_child(node);

			do {
				child_slot++;
			} while (child_slot < &parent->child[SLNODES] &&
				 !(*child_slot));

			/* need to go up */
			if (child_slot == &parent->child[SLNODES])
				return next_node(parent); // recursinve XXX

			return next_leaf(*child_slot);
		}

		tagged_node_ptr
		next_leaf(tagged_node_ptr n)
		{
			if (n.is_leaf())
				return n;

			if (n->leaf)
				return n->leaf;

			for (size_t i = 0; i < SLNODES; i++) {
				tagged_node_ptr m;
				if ((m = n->child[i]))
					return next_leaf(m);
			}

			/* there must be at least one leaf at the bottom */
			assert(false);
		}
	};

	/* Default ctor - constructs empty tree */
	radix_tree();

	/* Dtor - removes entire tree */
	~radix_tree();

	/*
	 * insert -- write a key:value pair to the radix tree
	 */
	template <class... Args>
	std::pair<iterator, bool> emplace(std::string_view k, Args &&... args);

	iterator find(std::string_view k);

	/*
	 * get -- query for a key ("==" match)
	 */
	bool get(string_view key);

	/*
	 * remove -- delete a key from the radit tree return true if element was
	 * present
	 */
	bool remove(obj::pool_base &pop, string_view key);

	// /*
	//  * iterate -- iterate over all leafs
	//  */
	// void iterate(pmemkv_get_kv_callback *callback, void *arg);

	/*
	 * size -- return number of elements
	 */
	uint64_t size();

private:
	int
	n_child(tagged_node_ptr n)
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

	tagged_node_ptr root;
	obj::p<uint64_t> size_;

	tagged_node_ptr& parent_ref(tagged_node_ptr n)
	{
		if (n.is_leaf())
			return n.get_leaf()->parent;

		return n->parent;
	}

	template <typename... Args>
	tagged_node_ptr
	make_leaf(tagged_node_ptr parent, Args &&... args)
	{
		assert(pmemobj_tx_stage() == TX_STAGE_WORK);

		size_++;

		auto ptr = make_persistent<leaf>(std::forward<Args>(args)...);
		ptr->parent = parent;

		return ptr;
	}

	leaf *any_bottom_leaf(tagged_node_ptr n);
	leaf *descend(string_view key);

	bool
	keys_equal(string_view k1, string_view k2)
	{
		return k1.size() == k2.size() && k1.compare(k2) == 0;
	}

	/*
	 * internal: slice_index -- return index of child at the given nib
	 */
	unsigned slice_index(char k, uint8_t shift);

	/*
	 * internal: delete_node -- recursively free (to malloc) a subtree
	 */
	void delete_node(tagged_node_ptr n);

	/*
	 * Helper method for iteration.
	//  */
	// void iterate_rec(tagged_node_ptr n, pmemkv_get_kv_callback *callback,
	// void *arg);
};

template <typename Value>
radix_tree<Value>::radix_tree() : root(nullptr), size_(0)
{

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

/*
 * any_bottom_leaf -- (internal) find any leaf in a subtree
 *
 * We know they're all identical up to the divergence point between a prefix
 * shared by all of them vs the new key we're inserting.
 */
template <typename Value>
typename radix_tree<Value>::leaf *
radix_tree<Value>::any_bottom_leaf(tagged_node_ptr n)
{
	for (size_t i = 0; i < SLNODES; i++) {
		tagged_node_ptr m;
		if ((m = n.get_node()->child[i]))
			return m.is_leaf() ? m.get_leaf() : any_bottom_leaf(m);
	}
	assert(false);
}

template <typename Value>
typename radix_tree<Value>::leaf *
radix_tree<Value>::descend(string_view key)
{
	auto n = root;

	while (!n.is_leaf() && n->byte < key.size()) {
		auto nn = n->child[slice_index(key.data()[n->byte], n->bit)];

		if (nn)
			n = nn;
		else {
			n = any_bottom_leaf(n);
			break;
		}
	}

	if (!n.is_leaf())
		n = any_bottom_leaf(n);

	return n.get_leaf();
}

static inline byten_t
prefix_diff(string_view lhs, string_view rhs)
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
std::pair<typename radix_tree<Value>::iterator, bool>
radix_tree<Value>::emplace(std::string_view key, Args &&... args)
{
	auto pop = pool_base(pmemobj_pool_by_ptr(this));

	if (!root) {
		obj::transaction::run(pop, [&] {
			root = make_leaf(nullptr, key, std::forward<Args>(args)...);
		});

		return {root, true};
	}

	/*
	 * Need to descend the tree twice: first to find a leaf that
	 * represents a subtree whose all keys share a prefix at least as
	 * long as the one common to the new key and that subtree.
	 */
	auto leaf = descend(key);
	auto diff = prefix_diff(key, leaf->data.key());

	/* Descend into the tree again. */
	auto n = root;
	auto parent = &root;
	auto prev = n;

	auto min_key_len = std::min(leaf->data.key().size(), key.size());

	bitn_t sh = 8 - SLICE;
	if (diff < leaf->data.key().size() && diff < key.size()) {
		auto at = static_cast<unsigned char>(leaf->data.key().data()[diff] ^
						     key.data()[diff]);
		sh = detail::mssb_index((uint32_t)at) & (bitn_t) ~(SLICE - 1);
	}

	while (n && !n.is_leaf() &&
	       (n->byte < diff ||
		(n->byte == diff &&
		 (n->bit > sh || (n->bit == sh && diff < min_key_len))))) {

		prev = n;
		parent = &n->child[slice_index(key.data()[n->byte], n->bit)];
		n = *parent;
	}

	/*
	 * If the divergence point is at same nib as an existing node, and
	 * the subtree there is empty, just place our leaf there and we're
	 * done.  Obviously this can't happen if SLICE == 1.
	 */
	if (!n) {
		assert(diff < leaf->data.key().size() && diff < key.size());

		obj::transaction::run(pop, [&] {
			*parent = make_leaf(prev, key, std::forward<Args>(args)...);
		});

		return {*parent, true};
	}

	/* New key is a prefix of the leaf key or they are equal. We need to add
	 * leaf ptr to internal node. */
	if (diff == key.size()) {
		if (n.is_leaf() && n.get_leaf()->data.key().size() == key.size()) {
			return {nullptr, false};
		}

		if (!n.is_leaf() && n->byte == key.size() && n->bit == 4) {
			if (n->leaf)
				return {nullptr, false};

			obj::transaction::run(pop, [&] {
				n->leaf = make_leaf(n,
					key, std::forward<Args>(args)...);
			});

			return {n->leaf, true};
		}

		tagged_node_ptr node;
		obj::transaction::run(pop, [&] {
			/* We have to add new node at the edge from parent to n
			 */
			node = obj::make_persistent<radix_tree::node>();
			node->leaf =
				make_leaf(node, key, std::forward<Args>(args)...);
			node->child[slice_index(leaf->data.key().data()[diff], sh)] =
				n;
			node->parent = parent_ref(n);
			node->byte = diff;
			node->bit = sh;

			parent_ref(n) = node;

			*parent = node;
		});

		return {node->leaf, true};
	}

	if (diff == leaf->data.key().size()) {
		/* Leaf key is a prefix of the new key. We need to convert leaf
		 * to a node.
		 */
		tagged_node_ptr node;
		obj::transaction::run(pop, [&] {
			/* We have to add new node at the edge from parent to n
			 */
			node = obj::make_persistent<radix_tree::node>();
			node->leaf = n;
			node->child[slice_index(key.data()[diff], sh)] =
				make_leaf(node, key, std::forward<Args>(args)...);
			node->parent = parent_ref(n);
			node->byte = diff;
			node->bit = sh;

			parent_ref(n) = node;

			*parent = node;
		});

		return {node->child[slice_index(key.data()[diff], sh)],true};
	}

	tagged_node_ptr node;
	obj::transaction::run(pop, [&] {
		/* We have to add new node at the edge from parent to n */
		node = obj::make_persistent<radix_tree::node>();
		node->child[slice_index(leaf->data.key().data()[diff], sh)] = n;
		node->child[slice_index(key.data()[diff], sh)] =
			make_leaf(node, key, std::forward<Args>(args)...);
		node->parent = parent_ref(n);
		node->byte = diff;
		node->bit = sh;

		parent_ref(n) = node;

		*parent = node;
	});

	return {node->child[slice_index(key.data()[diff], sh)],
		true};
}

template <typename Value>
typename radix_tree<Value>::iterator
radix_tree<Value>::find(std::string_view key)
{
	auto n = root;
	auto prev = n;
	while (n && !n.is_leaf()) {
		prev = n;
		if (n->byte == key.size() && n->bit == 4)
			n = n->leaf;
		else if (n->byte > key.size())
			return nullptr;
		else
			n = n->child[slice_index(key.data()[n->byte], n->bit)];
	}

	if (!n)
		return nullptr;

	if (key.compare(n.get_leaf()->data.key()) != 0)
		return nullptr;

	return n;
}

template <typename Value>
bool
radix_tree<Value>::remove(obj::pool_base &pop, string_view key)
{
	auto n = root;
	auto *parent = &root;
	decltype(parent) pp = nullptr;

	while (n && !n.is_leaf()) {
		pp = parent;

		if (n->byte == key.size() && n->bit == 4)
			parent = &n->leaf;
		else if (n->byte > key.size())
			return false;
		else
			parent = &n->child[slice_index(key.data()[n->byte],
						       n->bit)];

		n = *parent;
	}

	if (!n)
		return false;

	auto leaf = n.get_leaf();
	if (!keys_equal(key, leaf->data.key()))
		return false;

	obj::transaction::run(pop, [&] {
		obj::delete_persistent<radix_tree::leaf>(leaf);

		*parent = 0;
		size_--;

		/* was root */
		if (!pp)
			return;

		n = *pp;
		tagged_node_ptr only_child = nullptr;
		for (int i = 0; i < (int)SLNODES; i++) {
			if (n->child[i]) {
				if (only_child) {
					/* more than one child */
					return;
				}
				only_child = n->child[i];
			}
		}

		if (only_child && n->leaf && &n->leaf != parent) {
			/* there are actually 2 "childred" */
			return;
		} else if (n->leaf && &n->leaf != parent)
			only_child = n->leaf;

		assert(only_child);

		only_child->parent = n->parent;
		obj::delete_persistent<radix_tree::node>(n.get_node());
		*pp = only_child;
	});

	return true;
}

// void radix_tree::iterate_rec(radix_tree::tagged_node_ptr n,
// pmemkv_get_kv_callback *callback, 		       void *arg)
// {
// 	if (!n.is_leaf()) {
// 		assert(n_child(n) + bool((n)->leaf) > 1);

// 		if (n->leaf)
// 			iterate_rec(n->leaf, callback, arg);

// 		for (int i = 0; i < (int)SLNODES; i++) {
// 			if (n->child[i])
// 				iterate_rec(n->child[i], callback, arg);
// 		}
// 	} else {
// 		auto leaf = n.get_leaf();
// 		callback(leaf->data.key().data(), leaf->data.key().size(), leaf->data(),
// 			 leaf->value.size(), arg);
// 	}
// }

// void radix_tree::iterate(pmemkv_get_kv_callback *callback, void *arg)
// {
// 	if (root)
// 		iterate_rec(root, callback, arg);
// }

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
		obj::delete_persistent<radix_tree::node>(n.get_node());
	} else {
		size_--;
		obj::delete_persistent<radix_tree::leaf>(n.get_leaf());
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
	if (!rhs) {
		this->off = 0;
	} else {
		this->off =
			rhs.get<uint64_t>() - reinterpret_cast<uint64_t>(this);
		off |= unsigned(rhs.is_leaf());
	}
}

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
radix_tree<Value>::tagged_node_ptr::operator=(const tagged_node_ptr &rhs)
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

template <typename Value>
radix_tree<Value>::tagged_node_ptr::tagged_node_ptr(
	const obj::persistent_ptr<leaf> &ptr)
{
	if (!ptr) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(ptr.get()) -
			reinterpret_cast<uint64_t>(this);
		off |= 1U;
	}
}

template <typename Value>
radix_tree<Value>::tagged_node_ptr::tagged_node_ptr(
	const obj::persistent_ptr<node> &ptr)
{
	if (!ptr) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(ptr.get()) -
			reinterpret_cast<uint64_t>(this);
	}
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
radix_tree<Value>::tagged_node_ptr::operator=(
	const obj::persistent_ptr<leaf> &rhs)
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

template <typename Value>
typename radix_tree<Value>::tagged_node_ptr &
radix_tree<Value>::tagged_node_ptr::operator=(
	const obj::persistent_ptr<node> &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(rhs.get()) -
			reinterpret_cast<uint64_t>(this);
	}

	return *this;
}

template <typename Value>
bool
radix_tree<Value>::tagged_node_ptr::operator==(
	const radix_tree::tagged_node_ptr &rhs) const
{
	return get<uint64_t>() == rhs.get<uint64_t>() || (!*this && !rhs);
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
	return get<radix_tree::leaf *>();
}

template <typename Value>
typename radix_tree<Value>::node *
radix_tree<Value>::tagged_node_ptr::get_node() const
{
	assert(!is_leaf());
	return get<radix_tree::node *>();
}

template <typename Value>
radix_tree<Value>::tagged_node_ptr::operator bool() const noexcept
{
	return (off & ~uint64_t(1)) != 0;
}

template <typename Value>
typename radix_tree<Value>::node *
	radix_tree<Value>::tagged_node_ptr::operator->() const noexcept
{
	return get_node();
}

}
}