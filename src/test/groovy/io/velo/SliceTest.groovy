package io.velo

import spock.lang.Specification

import java.nio.ByteBuffer

class SliceTest extends Specification {

    def 'test base'() {
        when: 'default constructor'
        def slice = new Slice()

        then:
        slice.getWriteIndex() == 0
        slice.getReadIndex() == 0
        slice.readableBytes() == 0
        slice.getArray().length == 64
        !slice.isReadable()

        when: 'constructor with capacity'
        def slice128 = new Slice(128)
        then:
        slice128.getArray().length == 128

        when: 'constructor with invalid capacity'
        new Slice(0)
        then:
        thrown(IllegalArgumentException)

        when:
        new Slice(-1)
        then:
        thrown(IllegalArgumentException)

        when: 'constructor with byte array'
        def data = [1, 2, 3, 4, 5] as byte[]
        def fromArray = new Slice(data)
        then:
        fromArray.getWriteIndex() == 5
        fromArray.readableBytes() == 5
        fromArray.isReadable()
        fromArray.isReadable(5)
        !fromArray.isReadable(6)

        when: 'constructor with byte array offset and length'
        def fromOffset = new Slice(data, 2, 3)
        then:
        fromOffset.getWriteIndex() == 3
        fromOffset.getArray()[0] == 3
        fromOffset.getArray()[2] == 5

        when:
        new Slice(null, 0, 1)
        then:
        thrown(IllegalArgumentException)

        when:
        new Slice(data, -1, 1)
        then:
        thrown(IllegalArgumentException)

        when:
        new Slice(data, 0, 100)
        then:
        thrown(IllegalArgumentException)

        when: 'constructor with empty byte array'
        def emptySlice = new Slice(new byte[0])
        then:
        emptySlice.getWriteIndex() == 0
        emptySlice.readableBytes() == 0
        !emptySlice.isReadable()

        when:
        new Slice((byte[]) null)
        then:
        thrown(IllegalArgumentException)

        when: 'auto grow'
        def growSlice = new Slice(8)
        growSlice.writeBytes(new byte[8])
        growSlice.writeByte(1)
        then:
        growSlice.getArray().length >= 9
        growSlice.getWriteIndex() == 9

        when: 'readable bytes'
        def rSlice = new Slice()
        rSlice.writeInt(12345)
        then:
        rSlice.readableBytes() == 4
        rSlice.readInt()
        rSlice.readableBytes() == 0
    }

    def 'test write and read'() {
        given:
        def slice = new Slice()

        when: 'write byte'
        slice.writeByte(0x12)
        slice.writeByte(0x34)
        slice.writeByte(0xFF)
        then:
        slice.getWriteIndex() == 3
        slice.readByte() == 0x12
        slice.readByte() == 0x34
        slice.readByte() == 255
        slice.getReadIndex() == 3

        when: 'write bytes'
        def slice2 = new Slice()
        def bytes = [1, 2, 3, 4, 5] as byte[]
        slice2.writeBytes(bytes)
        slice2.writeBytes(bytes, 2, 2)
        then:
        slice2.getWriteIndex() == 7
        def dst = new byte[7]
        slice2.readBytes(dst)
        dst == [1, 2, 3, 4, 5, 3, 4] as byte[]

        when:
        new Slice().writeBytes((byte[]) null)
        then:
        thrown(IllegalArgumentException)

        when:
        new Slice().writeBytes(bytes, -1, 1)
        then:
        thrown(IllegalArgumentException)

        when:
        new Slice().readBytes((byte[]) null)
        then:
        thrown(IllegalArgumentException)

        when: 'write int big endian'
        def slice3 = new Slice()
        slice3.writeInt(0x12345678)
        slice3.writeInt(-1)
        then:
        slice3.getArray()[0] == 0x12
        slice3.getArray()[3] == 0x78
        slice3.readInt() == 0x12345678
        slice3.readInt() == -1

        when: 'write long big endian'
        def slice4 = new Slice()
        slice4.writeLong(0x123456789ABCDEF0L)
        slice4.writeLong(-1L)
        then:
        slice4.getArray()[0] == 0x12
        slice4.getArray()[7] == (byte) 0xF0
        slice4.readLong() == 0x123456789ABCDEF0L
        slice4.readLong() == -1L

        when: 'write double'
        def slice5 = new Slice()
        slice5.writeDouble(123.456d)
        then:
        slice5.getWriteIndex() == 8
        ByteBuffer.wrap(slice5.getArray(), 0, 8).getDouble() == 123.456d

        when: 'write short LE'
        def slice6 = new Slice()
        slice6.writeShortLE(0x1234)
        then:
        slice6.getArray()[0] == 0x34
        slice6.getArray()[1] == 0x12

        when: 'write int LE'
        def slice7 = new Slice()
        slice7.writeIntLE(0x12345678)
        then:
        slice7.getArray()[0] == 0x78
        slice7.getArray()[3] == 0x12

        when: 'write long LE'
        def slice8 = new Slice()
        slice8.writeLongLE(0x123456789ABCDEF0L)
        then:
        slice8.getArray()[0] == (byte) 0xF0
        slice8.getArray()[7] == 0x12

        when: 'mixed write and read'
        def mixed = new Slice()
        mixed.writeInt(12345678)
        mixed.writeLong(987654321012345L)
        mixed.writeBytes([104, 101, 108, 108, 111] as byte[])
        then:
        mixed.getWriteIndex() == 17

        when:
        def reader = new Slice(mixed.getArray(), 0, mixed.getWriteIndex())
        then:
        reader.readInt() == 12345678
        reader.readLong() == 987654321012345L
        def hello = new byte[5]
        reader.readBytes(hello)
        hello == [104, 101, 108, 108, 111] as byte[]
        reader.readableBytes() == 0
        !reader.isReadable()

        when: 'multiple writes with growth'
        def growth = new Slice(4)
        growth.writeInt(1)
        growth.writeInt(2)
        growth.writeInt(3)
        growth.writeInt(4)
        def growthReader = new Slice(growth.getArray(), 0, growth.getWriteIndex())
        then:
        growth.getWriteIndex() == 16
        growth.getArray().length >= 16
        growthReader.readInt() == 1
        growthReader.readInt() == 2
        growthReader.readInt() == 3
        growthReader.readInt() == 4
    }
}
