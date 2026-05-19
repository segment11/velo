package io.velo.metric


import spock.lang.Specification

class SimpleGaugeTest extends Specification {
    def 'concurrent collect and raw getter mutation does not throw'() {
        given:
        def g = new SimpleGauge('concurrent_test', 'test', 'label')
        g.register()

        def errors = Collections.synchronizedList(new ArrayList<Throwable>())
        def latch = new java.util.concurrent.CountDownLatch(2)
        def iterations = 500

        when:
        Thread.start {
            try {
                iterations.times {
                    g.clearRawGetterList()
                    g.addRawGetter {
                        Map<String, SimpleGauge.ValueWithLabelValues> map = new HashMap<>()
                        map.put('x' + it, new SimpleGauge.ValueWithLabelValues(1.0, ['v']))
                        map
                    }
                }
            } catch (Throwable t) {
                errors << t
            } finally {
                latch.countDown()
            }
        }

        Thread.start {
            try {
                iterations.times {
                    g.collect()
                }
            } catch (Throwable t) {
                errors << t
            } finally {
                latch.countDown()
            }
        }

        latch.await(10, java.util.concurrent.TimeUnit.SECONDS)

        then:
        errors.isEmpty()
    }

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

        when:
        g.clearRawGetterList()
        then:
        g.rawGetterList.size() == 0
    }
}
