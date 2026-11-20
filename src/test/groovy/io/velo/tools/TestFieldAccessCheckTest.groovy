package io.velo.tools

import spock.lang.Specification
import spock.lang.TempDir

import javax.tools.ToolProvider
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

class TestFieldAccessCheckTest extends Specification {

    @TempDir
    Path tempDir

    private Path createFixtureProject() {
        def javaSrc = tempDir.resolve('src/main/java')
        def mainClasses = tempDir.resolve('build/classes/java/main')
        def groovyTest = tempDir.resolve('src/test/groovy')
        Files.createDirectories(javaSrc)
        Files.createDirectories(mainClasses)
        Files.createDirectories(groovyTest)

        def alphaFile = writeText(javaSrc.resolve('io/velo/alpha/SampleHolder.java'), '''
package io.velo.alpha;

import java.util.ArrayList;
import java.util.List;

public class SampleHolder {
    public int publicCount = 1;
    int pkgCount = 2;
    protected String protectedTag = "tag";
    private final List<String> secretList = new ArrayList<>();
    private int hiddenIndex = 3;
    private static final int SECRET_HEADER = 16;

    public int getReadableCount() {
        return pkgCount;
    }

    int getHiddenIndex() {
        return hiddenIndex;
    }

    void setHiddenIndex(int hiddenIndex) {
        this.hiddenIndex = hiddenIndex;
    }

    protected String protectedHelper() {
        return protectedTag;
    }

    private void privateHelper(boolean flag) {
    }

    public List<SampleHolder> children() {
        return List.of(this);
    }

    public SampleHolder[] childArray() {
        return new SampleHolder[]{this};
    }

    int getVirtualCount() {
        return pkgCount;
    }

    void setVirtualCount(int v) {
        this.pkgCount = v;
    }

    public record SampleRecord(int code) {
    }
}
''')

        def subFile = writeText(javaSrc.resolve('io/velo/beta/SampleSub.java'), '''
package io.velo.beta;

import io.velo.alpha.SampleHolder;

public class SampleSub extends SampleHolder {
}
''')

        def compiler = ToolProvider.getSystemJavaCompiler()
        assert compiler != null
        def rc = compiler.run(null, null, null,
                '-d', mainClasses.toString(),
                alphaFile.toString(), subFile.toString())
        assert rc == 0

        // Cross-package test with violations
        writeText(groovyTest.resolve('io/velo/beta/BetaAccessTest.groovy'), '''
package io.velo.beta

import io.velo.alpha.SampleHolder

class BetaAccessTest {
    def 'test cross package violations'() {
        given:
        def h = new SampleHolder()
        def sub = new SampleSub()
        def list = h.children()
        def rec = new SampleHolder.SampleRecord(7)

        when:
        def ok1 = h.publicCount
        def ok2 = h.readableCount
        def ok3 = rec.code
        def bad1 = h.pkgCount
        h.pkgCount = 10
        h.pkgCount++
        def bad2 = h.protectedTag
        def bad3 = h.hiddenIndex
        h.hiddenIndex = 5
        def bad4 = sub.protectedTag
        def bad5 = list[0].pkgCount
        def methodBad1 = h.protectedHelper()
        h.privateHelper(true)

        then:
        ok1 == 1
        ok2 == 2
        ok3 == 7
        bad1 >= 0
        bad2 != null
        bad3 >= 0
        bad4 != null
        bad5 >= 0
        methodBad1 != null
    }
}
''')

        // Same-package test: package-private is legal, private and .@ are violations
        writeText(groovyTest.resolve('io/velo/alpha/AlphaAccessTest.groovy'), '''
package io.velo.alpha

class AlphaAccessTest {
    def 'test same package access'() {
        given:
        def h = new SampleHolder()

        when:
        def ok1 = h.pkgCount
        h.pkgCount = 9
        def ok2 = h.protectedTag
        def ok3 = h.hiddenIndex
        h.hiddenIndex = 4
        def ok4 = h.protectedHelper()
        def badPrivate = h.secretList
        def badStatic = SampleHolder.SECRET_HEADER
        def badDirect = h.@secretList
        h.privateHelper(false)

        then:
        ok1 >= 0
        ok2 != null
        ok3 >= 0
        ok4 != null
        badPrivate != null
        badStatic == 16
        badDirect != null
    }
}
''')

        // Clean test with only legal accesses
        writeText(groovyTest.resolve('io/velo/gamma/CleanTest.groovy'), '''
package io.velo.gamma

import io.velo.alpha.SampleHolder

class CleanTest {
    def 'test legal only'() {
        given:
        def h = new SampleHolder()

        expect:
        h.publicCount == 1
        h.readableCount == 2
    }
}
''')

        return tempDir
    }

    private static Path writeText(Path path, String content) {
        Files.createDirectories(path.parent)
        Files.writeString(path, content.stripIndent().trim() + '\n', StandardCharsets.UTF_8)
        return path
    }

    private static Map<String, Object> runCli(String... args) {
        def outBuf = new ByteArrayOutputStream()
        def errBuf = new ByteArrayOutputStream()
        int code = TestFieldAccessCheck.execute(
                args,
                new PrintStream(outBuf, true, StandardCharsets.UTF_8),
                new PrintStream(errBuf, true, StandardCharsets.UTF_8)
        )
        return [
                code: code,
                out : outBuf.toString(StandardCharsets.UTF_8),
                err : errBuf.toString(StandardCharsets.UTF_8)
        ]
    }

    def 'test detects cross-package, private, direct field, and method violations'() {
        given:
        def root = createFixtureProject()

        when:
        def res = new TestFieldAccessCheck().run(TestFieldAccessCheck.parseArgs([
                '--dir', root.toString(), '--include-methods', '--no-color'
        ] as String[]))

        then:
        res.filesScanned() == 3
        res.classesLoaded() >= 3

        // Cross-package findings from BetaAccessTest
        res.fieldFindings().any {
            it.targetClass() == 'io.velo.alpha.SampleHolder' &&
                    it.memberName() == 'pkgCount' &&
                    it.category() == TestFieldAccessCheck.Category.CROSS_PACKAGE &&
                    it.accessKind() == 'READ'
        }
        res.fieldFindings().any {
            it.targetClass() == 'io.velo.alpha.SampleHolder' &&
                    it.memberName() == 'pkgCount' &&
                    it.category() == TestFieldAccessCheck.Category.CROSS_PACKAGE &&
                    it.accessKind() == 'WRITE'
        }
        res.fieldFindings().any {
            it.targetClass() == 'io.velo.alpha.SampleHolder' &&
                    it.memberName() == 'protectedTag' &&
                    it.category() == TestFieldAccessCheck.Category.CROSS_PACKAGE
        }
        res.fieldFindings().any {
            it.targetClass() == 'io.velo.alpha.SampleHolder' &&
                    it.memberName() == 'hiddenIndex' &&
                    it.category() == TestFieldAccessCheck.Category.CROSS_PACKAGE &&
                    it.accessKind() == 'READ'
        }
        res.fieldFindings().any {
            it.targetClass() == 'io.velo.alpha.SampleHolder' &&
                    it.memberName() == 'hiddenIndex' &&
                    it.category() == TestFieldAccessCheck.Category.CROSS_PACKAGE &&
                    it.accessKind() == 'WRITE'
        }

        // Same-package private & direct .@ findings from AlphaAccessTest
        res.fieldFindings().any {
            it.targetClass() == 'io.velo.alpha.SampleHolder' &&
                    it.memberName() == 'secretList' &&
                    it.category() == TestFieldAccessCheck.Category.PRIVATE_FIELD
        }
        res.fieldFindings().any {
            it.targetClass() == 'io.velo.alpha.SampleHolder' &&
                    it.memberName() == 'SECRET_HEADER' &&
                    it.category() == TestFieldAccessCheck.Category.PRIVATE_FIELD
        }
        res.fieldFindings().any {
            it.targetClass() == 'io.velo.alpha.SampleHolder' &&
                    it.memberName() == 'secretList' &&
                    it.category() == TestFieldAccessCheck.Category.DIRECT_FIELD
        }

        // Legal accesses are NOT flagged
        !res.fieldFindings().any { it.memberName() == 'publicCount' }
        !res.fieldFindings().any { it.memberName() == 'readableCount' }
        !res.fieldFindings().any { it.memberName() == 'code' }
        !res.fieldFindings().any { it.testFile().contains('AlphaAccessTest') && it.memberName() == 'pkgCount' }
        !res.fieldFindings().any { it.testFile().contains('AlphaAccessTest') && it.memberName() == 'hiddenIndex' }
        !res.fieldFindings().any { it.testFile().contains('CleanTest') }

        // Method findings
        res.methodFindings().any { it.memberName() == 'protectedHelper(0 args)' && it.testFile().contains('BetaAccessTest') }
        res.methodFindings().any { it.memberName() == 'privateHelper(1 args)' && it.testFile().contains('AlphaAccessTest') }
        !res.methodFindings().any { it.memberName() == 'protectedHelper(0 args)' && it.testFile().contains('AlphaAccessTest') }
    }

    def 'test CLI modes, filters, pretty formatting, and exit codes'() {
        given:
        def root = createFixtureProject()

        when: 'clean test file passes with exit code 0'
        def clean = runCli('CleanTest', '--dir', root.toString(), '--no-color')

        then:
        clean.code == 0
        (clean.out as String).contains('[OK]')
        (clean.out as String).contains('No non-accessible field/property accesses found.')

        when: 'cross-package-only filter excludes same-package private hits'
        def crossOnly = runCli('--dir', root.toString(), '--cross-package-only', '--no-color', '--src')

        then:
        crossOnly.code == 1
        (crossOnly.out as String).contains('[CROSS-PKG]')
        !(crossOnly.out as String).contains('secretList')
        (crossOnly.out as String).contains('def bad1 = h.pkgCount')

        when: 'by-file grouping and target-class filter with ANSI colors'
        def byFile = runCli('--dir', root.toString(), '--by-file', '--target-class', 'SampleHolder', '--include-methods', '--color')

        then:
        byFile.code == 1
        (byFile.out as String).contains('BetaAccessTest.groovy')
        (byFile.out as String).contains('AlphaAccessTest.groovy')
        (byFile.out as String).contains('protectedHelper')
        (byFile.out as String).contains('\u001B[')

        when: 'package filter matches only io.velo.alpha'
        def pkgOnly = runCli('io.velo.alpha', '--dir', root.toString(), '--no-color')

        then:
        pkgOnly.code == 1
        (pkgOnly.out as String).contains('AlphaAccessTest.groovy')
        !(pkgOnly.out as String).contains('BetaAccessTest.groovy')

        when: 'help flag exits 0'
        def help = runCli('--help')

        then:
        help.code == 0
        (help.out as String).contains('Usage:')

        when: 'unknown option exits 2'
        def badArg = runCli('--does-not-exist')

        then:
        badArg.code == 2
        (badArg.err as String).contains('Unknown option')

        when: 'missing compiled classes directory exits 2'
        def emptyDir = tempDir.resolve('empty')
        Files.createDirectories(emptyDir)
        def noClasses = runCli('--dir', emptyDir.toString(), '--no-color')

        then:
        noClasses.code == 2
        (noClasses.err as String).contains('No compiled classes found')

        when: 'missing option values, extra positional arg, or unmatched target exit 2'
        def missingDir = runCli('--dir')
        def missingTargetClass = runCli('--target-class')
        def extraPos = runCli('CleanTest', 'ExtraArg')
        def unmatched = runCli('NonExistentTest', '--dir', root.toString(), '--no-color')

        then:
        missingDir.code == 2
        missingTargetClass.code == 2
        extraPos.code == 2
        unmatched.code == 2
        (unmatched.err as String).contains('No Groovy test files matched target')
    }

    def 'test star imports, helpers, loops, closures, casts, arrays, and virtual accessors'() {
        given:
        def root = createFixtureProject()
        writeText(root.resolve('src/test/groovy/io/velo/delta/DeltaFlowTest.groovy'), '''
package io.velo.delta

import io.velo.alpha.*

class DeltaFlowTest {
    private SampleHolder fieldHolder = new SampleHolder()

    private SampleHolder makeHolder() {
        return new SampleHolder()
    }

    private def makeContext() {
        def h = new SampleHolder()
        return [holder: h]
    }

    def 'test complex inference paths'() {
        given:
        def ctx = makeContext()
        def helper = makeHolder()
        def arr = helper.childArray()
        Object rawObj = helper
        def casted = (SampleHolder) rawObj

        when:
        def v1 = ctx.holder.pkgCount
        def v2 = arr[0].pkgCount
        def v3 = casted.pkgCount
        def v4 = fieldHolder.virtualCount
        fieldHolder.virtualCount = 8
        helper.pkgCount += 2
        ++helper.pkgCount
        for (item in helper.children()) {
            def x = item.pkgCount
        }
        helper.children().each {
            def y = it.pkgCount
        }
        helper.childArray().each { SampleHolder elem ->
            def z = elem.pkgCount
        }

        then:
        v1 >= 0
        v2 >= 0
        v3 >= 0
        v4 >= 0
    }
}
''')

        when:
        def res = new TestFieldAccessCheck().run(TestFieldAccessCheck.parseArgs([
                'DeltaFlowTest', '--dir', root.toString(), '--no-color'
        ] as String[]))

        then:
        res.filesScanned() == 1
        res.fieldFindings().any { it.memberName() == 'pkgCount' && it.expressionText().contains('ctx.holder') }
        res.fieldFindings().any { it.memberName() == 'pkgCount' && it.expressionText().contains('arr[0]') }
        res.fieldFindings().any { it.memberName() == 'pkgCount' && it.expressionText().contains('casted') }
        res.fieldFindings().any { it.memberName() == 'pkgCount' && it.expressionText().contains('item') }
        res.fieldFindings().any { it.memberName() == 'pkgCount' && it.expressionText().contains('it') }
        res.fieldFindings().any { it.memberName() == 'pkgCount' && it.expressionText().contains('elem') }
        res.fieldFindings().any { it.memberName() == 'virtualCount' && it.accessKind() == 'READ' }
        res.fieldFindings().any { it.memberName() == 'virtualCount' && it.accessKind() == 'WRITE' }
    }
}
