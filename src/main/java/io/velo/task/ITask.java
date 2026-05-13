package io.velo.task;

/**
 * Task interface for executable units with name and loop control.
 */
public interface ITask {
    /** @return the task name */
    String name();

    /** Executes the task logic. */
    void run();

    /**
     * @param loopCount the number of times the task has looped
     */
    default void setLoopCount(long loopCount) {
    }

    /**
     * @return execution interval (loops between executions), defaults to 1
     */
    default int executeOnceAfterLoopCount() {
        return 1;
    }
}