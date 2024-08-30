package io.velo.task;

public interface ITask {
    String name();

    void run();

    void setLoopCount(int loopCount);

    default int executeOnceAfterLoopCount() {
        return 1;
    }
}
