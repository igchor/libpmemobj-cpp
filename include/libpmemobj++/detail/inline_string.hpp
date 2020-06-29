#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/transaction.hpp>

#include <libpmemobj++/detail/pair.hpp>
#include <libpmemobj++/slice.hpp>

#include <string>

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

	std::string_view
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

// wrapper???

// XXX - reimplement this pair
template <typename Value>
struct inline_string_pair : public detail::pair<detail::inline_string, Value> {
	using detail::pair<detail::inline_string, Value>::pair;

	std::string_view
	first()
	{
		auto r = detail::pair<detail::inline_string, Value>::first.data(
			reinterpret_cast<char *>(this + 1));
		return {r.begin(), r.size()};
	}

	Value &
	second()
	{
		return detail::pair<detail::inline_string, Value>::second;
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

template <typename Data>
struct inline_pair

	template <typename Value, typename Pointer>
	struct tree_leaf {
	template <typename... Args>
	tree_leaf(Args &&... args) : data(std::forward<Args>(args)...)
	{
	}

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
// v.size(),
// detail::type_num<inline_pair<detail::inline_string>>(), 0);

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

}
}