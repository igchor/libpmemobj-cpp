persistent_ptr<foo> foo_ptr;
p<persistent_ptr<foo>> pmem_foo_ptr;
p<int> pint;
int simple_int;

transaction::run([&] {
    pint = 5;                               // ok, snapshotted
    foo_ptr = make_persistent<>();          // ok (but dangerous, not snapshotted)
    pmem_foo_ptr = make_persistent<>();     // ok, snapshotted
    make_persistent_atomic(foo_ptr);        // ok?
    make_persistent_atomic(pmem_foo_ptr);   // error, atomic allocation inside tx
    simple_int = 5;                         // ok, not p<>
});

// treat all PMEM variables (p, p<persistent_ptr>) as atomic
unsafe::run([&]{
    pint = 5;                               // ok, atomic modification
    foo_ptr = make_persistent<>();          // error, make_persistent must be inside tx
    pmem_foo_ptr = make_persistent<>();     // error, make_persistent must be inside tx
    make_persistent_atomic(foo_ptr);        // ok
    make_persistent_atomic(pmem_foo_ptr);   // ok
    simple_int = 5;                         // ok
});

pint = 5;                               // error, must be inside tx or unsafe scope
foo_ptr = make_persistent<>();          // error, must be inside tx scope
pmem_foo_ptr = make_persistent<>();     // error, must be inside tx scope (p<persistent_ptr<>> only on pmem)
make_persistent_atomic(foo_ptr);        // ok, atomic modification of simple persistent_ptr
make_persistent_atomic(pmem_foo_ptr);   // error, atomic modification of pmem_persistent_ptr must be inside unsafe scope
simple_int = 5;                         // ok
