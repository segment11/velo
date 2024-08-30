package io.velo.metric


import spock.lang.Specification

class SimpleGaugeTest extends Specification {
    def 'test add getter and collect'() {
        given:
        def g = new SimpleGauge('test', 'test', 'slot')

        def labelValues = ['0']
        def labelValues2 = ['1']

        when:
        g.register()
        g.set('123', 123.0, '0')
        g.addRawGetter {
            Map<String, SimpleGauge.ValueWithLabelValues> map = [:]
            map.put('a', new SimpleGauge.ValueWithLabelValues(1.0, labelValues))
            map.put('b', new SimpleGauge.ValueWithLabelValues(2.0, labelValues2))
            map
        }

        g.addRawGetter {
            Map<String, SimpleGauge.ValueWithLabelValues> map = [:]
            map.put('c', new SimpleGauge.ValueWithLabelValues(3.0, labelValues2))
            map
        }

        def mfsList = g.collect()

        then:
        g.rawGetterList.size() == 2
        mfsList.size() == 1
        mfsList[0].name == 'test'
        mfsList[0].samples.size() == 4
    }
}
