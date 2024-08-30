# velo supports hash / list / set / zset 4 types of data structures.

## Hash

velo store hash in two ways:

- key + field is a unique key, a hash has many key + field pairs.
- all fields + values are stored in a single key.

when a hash use one key, fields + values can be compressed by a trained dictionary.
when a hash use many key + field pairs, the field value can be compressed by a trained dictionary.

## List

velo store a list in a single key, all list members are stored as a compressed value.
max list members is 1024 or 2048 by config.

## Set

same as list, store all set members in a single key.

## ZSet

same as list, store all zset members and scores in a single key.