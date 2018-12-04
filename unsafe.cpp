persistent_ptr<foo> foo_ptr;
p<persistent_ptr<foo>> pmem_foo_ptr;
p<int> pint;

transaction::run([&] {
    pint = 5;                               // ok, snapshotted
    foo_ptr = make_persistent<>();          // ok?
    pmem_foo_ptr = make_persistent<>();     // ok
    make_persistent_atomic(foo_ptr);        // ok?
    make_persistent_atomic(pmem_foo_ptr);   // error
});

// treat all PMEM variables (p, p<persistent_ptr>) as atomic
unsafe::run([&]{
    pint = 5;                               // ok, atomic modification
    foo_ptr = make_persistent<>();          // error
    pmem_foo_ptr = make_persistent<>();     // error
    make_persistent_atomic(foo_ptr);        // ok
    make_persistent_atomic(pmem_foo_ptr);   // ok
});

pint = 5;                               // error
foo_ptr = make_persistent<>();          // error
pmem_foo_ptr = make_persistent<>();     // error (p<persistent_ptr<>> only on pmem)
make_persistent_atomic(foo_ptr);        // ok
make_persistent_atomic(pmem_foo_ptr);   // error

