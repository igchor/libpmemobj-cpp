Unsafe function for p/persistent_ptr/containers/iterators

unsafe() function returns reference to an object which does not call add_to_tx().

Compatibility issue (we change default behaviour):
1. Create new namespace (duplication of code) - old version is maintanace only
2. Create template parameter for p/persistent_ptr/containers? - 
   new features need to be tested on both versions (using enable_if for new functions?)


Global switch for add_to_tx - what if multiple modules (which use one another)
are compiled with different option? 


--------------------------------------------------------------------------------
USAGE:
--------------------------------------------------------------------------------

p<int> pint;
persistent_ptr<int> ptr;

tx::run([&]
{
	p<int> pint = 5; // ok, modification on stack
	ptr = make_persistent<>(); // ok, modification on stack
				   // possible another approach:

	// transient_ptr (which can be stored on stack)
	// persistent_ptr (which can only be stored on pmem)
	// transient_ptr must be assign to persistent_ptr (this assignemnts
	// zeroes a transient_ptr), not assigning will result in error
	// in tx_end (throw from a destructor?)
});

{
	p<int> pint = 5; // ok, modification on stack
}

{
	root->pint = 5; // error, modification outside tx
	root->pint.unsafe_get() = 5; // ok

	root->ptr.unsafe_get() = root->other_ptr;
}

tx::run([&]
{
	root->pint = 5; // ok, modification snapshotted
	root->pint.unsafe_get() = 6; // ok, modifying already snapshotted value
});

-------------------------------- Containers ------------------------------------
vector<int> v;

{
	v[5] = 10; // error - outside transaction
	v.unsafe_get(5) = 10; // ok

	auto it = v.begin();
	*it = 5; // error - outside transaction

	it.unsafe_get() = 5; // ok

	v.push_back(10); // ok, starts its own transaction
}