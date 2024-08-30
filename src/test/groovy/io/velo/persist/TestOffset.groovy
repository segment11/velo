package io.velo.persist

int offset = 22548

int diff = offset - 8
println diff % 70

int pageSize = 4096

int page = offset / pageSize
println page

println 22548 - page * 4096
println 22548 % pageSize
