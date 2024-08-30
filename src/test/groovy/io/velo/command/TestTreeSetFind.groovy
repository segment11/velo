package io.velo.command

import io.velo.type.RedisZSet

TreeSet<RedisZSet.ScoreValue> set = []

println set.add(new RedisZSet.ScoreValue(1, "a"))
println set.add(new RedisZSet.ScoreValue(2, "a"))
println set.add(new RedisZSet.ScoreValue(2, "b"))
println set.add(new RedisZSet.ScoreValue(3, "c"))

println set
println set.contains(new RedisZSet.ScoreValue(1, "a"))
println set.contains(new RedisZSet.ScoreValue(2, "a"))
//println set.remove(new RedisZSet.ScoreValue(2, "c"))
println set.ceiling(new RedisZSet.ScoreValue(Double.NEGATIVE_INFINITY, "a"))
println set.ceiling(new RedisZSet.ScoreValue(Double.NEGATIVE_INFINITY, "c"))

println set

println '-----------------'
def rz = new RedisZSet()
println rz.add(1, 'a')
println rz.add(2, 'a')
println rz.add(2, 'b')
println rz.add(3, 'b', false)
println rz.add(3, 'c')

println rz.size()
rz.print()

println '-----------------'
println rz.contains('a')
println rz.contains('b')
println rz.contains('c')
println rz.contains('d')

println '-----------------'
rz.remove('a')
println rz.size()
rz.print()


