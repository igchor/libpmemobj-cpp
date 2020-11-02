set width 0
set height 0
set verbose off
set confirm off
set breakpoint pending on
set pagination off
set print address off

break concurrent_map_mt_gdb.cpp:16
run
thread 3
set scheduler-locking on
break concurrent_map_mt_gdb.cpp:14
c
c
c
c
c
c
c
quit
