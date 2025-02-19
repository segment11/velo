package io.velo.task;

/**
 * Interface representing a task that can be executed.
 * Implementations of this interface should define the logic
 * for a task's name, how to run the task, and how many times
 * the task should loop before executing additional logic.
 */
public interface ITask {
    /**
     * Returns the name of the task.
     *
     * @return the name of the task as a String
     */
    String name();

    /**
     * Executes the task. This method contains the core logic
     * that defines what the task does when run.
     */
    void run();

    /**
     * Sets the number of times the task should loop.
     *
     * @param loopCount the number of times the task should loop
     */
    void setLoopCount(int loopCount);

    /**
     * Returns the number of times the task should execute
     * its logic after the loop count is reached.
     * <p>
     * By default, this method returns 1, indicating that the task
     * should execute every loop.
     *
     * @return the number of times the task should execute after the loop
     */
    default int executeOnceAfterLoopCount() {
        return 1;
    }
}