package io.velo.acl

import io.velo.BaseCommand
import io.velo.command.AGroup
import io.velo.command.BGroup
import io.velo.command.CGroup
import io.velo.command.DGroup
import io.velo.command.EGroup
import io.velo.command.FGroup
import io.velo.command.GGroup
import io.velo.command.HGroup
import io.velo.command.IGroup
import io.velo.command.JGroup
import io.velo.command.KGroup
import io.velo.command.LGroup
import io.velo.command.MGroup
import io.velo.command.NGroup
import io.velo.command.OGroup
import io.velo.command.PGroup
import io.velo.command.QGroup
import io.velo.command.RGroup
import io.velo.command.SGroup
import io.velo.command.TGroup
import io.velo.command.UGroup
import io.velo.command.VGroup
import io.velo.command.WGroup
import io.velo.command.XGroup
import io.velo.command.YGroup
import io.velo.command.ZGroup
import io.velo.command.CommandRegistry
import spock.lang.Specification

class CategoryTest extends Specification {
    def 'test all'() {
        expect:
        Category.getCmdListByCategory(Category.dangerous).size() > 0
        Category.getCategoryListByCmd('acl').size() > 0
        Category.isWriteCmd('set')
    }

    def 'test every command registered as @write is classified as a write command'() {
        when:
        // reference every group class to trigger static initialization
        new AGroup(null, null, null)
        new BGroup(null, null, null)
        new CGroup(null, null, null)
        new DGroup(null, null, null)
        new EGroup(null, null, null)
        new FGroup(null, null, null)
        new GGroup(null, null, null)
        new HGroup(null, null, null)
        new IGroup(null, null, null)
        new JGroup(null, null, null)
        new KGroup(null, null, null)
        new LGroup(null, null, null)
        new MGroup(null, null, null)
        new NGroup(null, null, null)
        new OGroup(null, null, null)
        new PGroup(null, null, null)
        new QGroup(null, null, null)
        new RGroup(null, null, null)
        new SGroup(null, null, null)
        new TGroup(null, null, null)
        new UGroup(null, null, null)
        new VGroup(null, null, null)
        new WGroup(null, null, null)
        new XGroup(null, null, null)
        new YGroup(null, null, null)
        new ZGroup(null, null, null)

        def missing = CommandRegistry.all()
                .findAll { !it.name().contains('|') && it.aclCategories().contains('@write') }
                .collect { it.name().toLowerCase() }
                .findAll { !Category.isWriteCmd(it) }

        then:
        missing.isEmpty()
    }

    def 'test read only key pattern does not grant write commands'() {
        given:
        def u = new U('ro')
        u.setOn(true)
        u.rCmdList << RCmd.fromLiteral('+get')
        u.rCmdList << RCmd.fromLiteral('+lrange')
        u.rCmdList << RCmd.fromLiteral('+lmpop')
        u.rCmdList << RCmd.fromLiteral('+hgetdel')
        u.rKeyList << RKey.fromLiteral('%R~k')

        def slotList = [BaseCommand.slot('k'.bytes, (short) 1)]

        expect: 'reads are allowed'
        u.checkCmdAndKey('get', data('get', 'k'), slotList).asBoolean()
        u.checkCmdAndKey('lrange', data('lrange', 'k', '0', '-1'), slotList).asBoolean()

        and: 'but write commands are denied'
        ['set', 'lpush', 'lpop', 'rpop', 'lmpop', 'blmpop', 'zmpop', 'bzmpop', 'hgetdel'].every { cmd ->
            !u.checkCmdAndKey(cmd, data(cmd, 'k'), slotList).asBoolean()
        }
    }

    def 'test write only key pattern does not grant read write commands'() {
        given:
        def u = new U('wo')
        u.setOn(true)
        u.rCmdList << RCmd.fromLiteral('+get')
        u.rCmdList << RCmd.fromLiteral('+set')
        u.rCmdList << RCmd.fromLiteral('+lpop')
        u.rCmdList << RCmd.fromLiteral('+lmpop')
        u.rKeyList << RKey.fromLiteral('%W~k')

        def slotList = [BaseCommand.slot('k'.bytes, (short) 1)]

        expect: 'plain writes are allowed, reads are denied'
        u.checkCmdAndKey('set', data('set', 'k', 'v'), slotList).asBoolean()
        !u.checkCmdAndKey('get', data('get', 'k'), slotList).asBoolean()

        and: 'read write commands need both key permissions'
        ['lpop', 'lmpop'].every { cmd ->
            !u.checkCmdAndKey(cmd, data(cmd, 'k'), slotList).asBoolean()
        }
    }

    private static byte[][] data(String cmd, String... args) {
        def d = new byte[args.length + 1][]
        d[0] = cmd.bytes
        for (int i = 0; i < args.length; i++) {
            d[i + 1] = args[i].bytes
        }
        return d
    }
}
