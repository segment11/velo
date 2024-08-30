# velo support 3 kinds of data lru

## latest get compress value

include big string type.

user can change lru max size dynamically when server is running, according to the memory usage and cache hit rate.

## key buckets compressed

user can config key buckets lru max size for one slot, max is 512K.

## chunk segments

optional.