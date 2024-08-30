package io.velo.command

import io.velo.type.RedisZSet

TreeSet<RedisZSet.ScoreValue> set = []

println set.add(new RedisZSet.ScoreValue(2, "a"))
println set.add(new RedisZSet.ScoreValue(4, "b"))
println set.add(new RedisZSet.ScoreValue(6, "c"))

println set