import io.velo.BaseCommand
import io.velo.ConfForSlot
import io.velo.KeyHash

def n = 100_000
int slotNumber = 8

ConfForSlot.global = ConfForSlot.c10m

n.times { i ->
    def key = 'key:' + i
    def keyPadding = key.padRight(16, '0')

    def keyHash = KeyHash.hash(keyPadding.bytes)
    if (keyHash < 0) {
        println BaseCommand.slot(keyPadding.bytes, slotNumber)
    }
}
