package io.velo.task


import spock.lang.Specification

class TaskChainTest extends Specification {
    static class Task1 implements ITask {
        @Override
        String name() {
            'task1'
        }

        @Override
        void run() {
            if (loopCount == 8) {
                throw new RuntimeException('test exception')
            }
            println 'task1, loop count: ' + loopCount
        }

        long loopCount

        @Override
        void setLoopCount(long loopCount) {
            this.loopCount = loopCount
        }

        @Override
        int executeOnceAfterLoopCount() {
            2
        }
    }

    static class Task2 extends Task1 {
        @Override
        String name() {
            'task2'
        }
    }

    static class Task3 implements ITask {
        @Override
        String name() {
            'task3'
        }

        @Override
        void run() {
            println 'task3, loop count: ' + loopCount
        }

        long loopCount

        @Override
        void setLoopCount(long loopCount) {
            this.loopCount = loopCount
        }
    }

    def 'test all'() {
        given:
        def taskChain = new TaskChain()
        def task1 = new Task1()
        def task2 = new Task2()
        def task3 = new Task3()

        println task3.executeOnceAfterLoopCount()

        when:
        taskChain.add(task1)
        taskChain.add(task1)
        taskChain.add(task2)
        println taskChain
        then:
        taskChain.list.size() == 2

        when:
        taskChain.remove('task2')
        10.times {
            taskChain.doTask(it)
        }
        then:
        task1.loopCount == 8

        when:
        taskChain.remove('task1')
        taskChain.remove('task1')
        then:
        taskChain.list.size() == 0
    }
}
