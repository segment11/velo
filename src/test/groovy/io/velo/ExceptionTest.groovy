package io.velo


import io.velo.persist.BucketFullException
import io.velo.persist.CannotReadException
import io.velo.persist.ReadonlyException
import io.velo.persist.SegmentOverflowException
import spock.lang.Specification

class ExceptionTest extends Specification {
    // just for coverage
    def 'test constructor'() {
        when:
        new DictMissingException()
        new TypeMismatchException('xxx')
        new BucketFullException('xxx')
        new SegmentOverflowException('xxx')
        new ReadonlyException()

        then:
        1 == 1
    }

    def 'test the exceptions without a message argument carry a message'() {
        expect: 'the error reply of a command is built from the exception message'
        new ReadonlyException().getMessage() != null
        new CannotReadException().getMessage() != null
    }
}
