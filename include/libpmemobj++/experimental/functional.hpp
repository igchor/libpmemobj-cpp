#include "contiguous_iterator.hpp"

namespace pmem
{
namespace obj
{
namespace experimental
{

template <class T>
using Predicate = std::function<bool(const T&)>;

/**
 * Const iterator.
 */
template <typename T>
struct filtering_iterator
    : public contiguous_iterator<filtering_iterator<T>, T &,
				 T *>,
      public operator_base<T> {
	using iterator_category = std::random_access_iterator_tag; // XXX
	using value_type = T;
	using difference_type = std::ptrdiff_t;
	using reference = T &;
	using pointer = T *;
	using base_type = contiguous_iterator<filtering_iterator<T>,
					      reference, pointer>;

    static constexpr bool
    always_true (const T&)
    {
        return true;
    }

	/**
	 * Constructor taking pointer as argument.
	 */
	filtering_iterator(pointer ptr = nullptr, Predicate<value_type> predicate = always_true) : base_type(ptr), predicate(predicate)
	{
        while (!predicate(*ptr))
            ptr++;
	}

	/**
	 * Non-member swap function.
	 */
	friend void
	swap(filtering_iterator &lhs, filtering_iterator &rhs)
	{
		std::swap(lhs.ptr, rhs.ptr);
        std::swap(lhs.predicate, rhs.predicate);
	}

protected:
    void
	change_by(std::ptrdiff_t n)
	{
        do {
            ptr++;

            if (predicate(*ptr)
                n--;
        } while (n > 0);
	}

private:
    Predicate<value_type> predicate;
};

namespace functional
{
    template <typename T>
    struct filtered 
    {
        filtered(T predicate): predicate(predicate)
        {
        }

        T predicate;
    };
}

template <typename T>
slice<filter_iterator<T::iterator::value_type>> filter(T range, Predicate<T::iterator::value_type> predicate)
{
	return slice(
        filter_iterator<T::iterator::value_type>(range.begin(), predicate),
        filter_iterator<T::iterator::value_type>(range.end(), predicate));
}

template <typename T>
slice<filter_iterator<T::iterator::value_type>> operator|(T range, functional::filtered<Predicate<T::iterator::value_type>> filtered)
{
	
}

}
}
}