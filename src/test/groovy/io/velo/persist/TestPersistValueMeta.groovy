package io.velo.persist

def pvm = new PersistValueMeta()
println pvm

def bytes = pvm.encode()

def pvm2 = PersistValueMeta.decode(bytes)
println pvm2
