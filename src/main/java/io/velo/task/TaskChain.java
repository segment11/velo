package io.velo.task;

import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Represents a chain of tasks to be executed based on a loop count.
 */
public class TaskChain {
    private static final Logger log = LoggerFactory.getLogger(TaskChain.class);

    /*
     * Need not thread safe, because it is in one slot (thread safe).
     */
    private final ArrayList<ITask> list = new ArrayList<>();

    /**
     * Gets the list of tasks in the chain. This method is visible for testing purposes.
     *
     * @return the list of tasks
     */
    @VisibleForTesting
    public ArrayList<ITask> getList() {
        return list;
    }

    /**
     * Returns a string representation of the TaskChain, including the size and names of tasks.
     *
     * @return a string representation of the TaskChain
     */
    @Override
    public String toString() {
        return "TaskChain{" +
                "list.size=" + list.size() +
                ", list.names=" + list.stream().map(ITask::name).reduce((a, b) -> a + "," + b).orElse("") +
                '}';
    }

    /**
     * Executes tasks in the chain that should run on the given loop count.
     *
     * @param loopCount the current loop count
     */
    public void doTask(int loopCount) {
        for (var t : list) {
            if (loopCount % t.executeOnceAfterLoopCount() == 0) {
                t.setLoopCount(loopCount);

                try {
                    t.run();
                } catch (Exception e) {
                    log.error("Task error, name={}", t.name(), e);
                }
            }
        }
    }

    /**
     * Adds a task to the chain if no task with the same name already exists.
     *
     * @param task the task to add
     */
    public void add(ITask task) {
        for (ITask t : list) {
            if (t.name().equals(task.name())) {
                return;
            }
        }

        list.add(task);
    }

    /**
     * Removes the task with the specified name from the chain.
     *
     * @param name the name of the task to remove
     * @return the removed task, or null if no task with the specified name was found
     */
    public ITask remove(String name) {
        var it = list.iterator();
        while (it.hasNext()) {
            var t = it.next();
            if (t.name().equals(name)) {
                it.remove();
                return t;
            }
        }
        return null;
    }
}
