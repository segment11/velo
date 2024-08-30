package io.velo.persist

def f = new File('/tmp/velo-data/jmap.txt')

def lines = f.readLines()[2..-1].collect {
    it.split(/\s+/)[3] as long
}

lines.each {
    println it
}
