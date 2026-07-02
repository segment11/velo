package io.velo.task;

import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * Chain of tasks executed based on loop count.
 */
public class TaskChain {
    private static final Logger log = LoggerFactory.getLogger(TaskChain.class);

    /*
     * Need not thread safe, because it is in one slot (thread safe).
     */
    private final ArrayList<ITask> list = new ArrayList<>();

    /** @return the list of tasks in the chain */
    @VisibleForTesting
    public ArrayList<ITask> getList() {
        return list;
    }

    /** @return string representation with size and task names */
    @Override
    public String toString() {
        return "TaskChain{" +
                "list.size=" + list.size() +
                ", list.names=" + list.stream().map(ITask::name).reduce((a, b) -> a + "," + b).orElse("") +
                '}';
    }

    /**
     * Executes tasks in the chain on the given loop count.
     *
     * @param loopCount the current loop count
     */
    public void doTask(long loopCount) {
        for (var t : list) {
            var every = t.executeOnceAfterLoopCount();
            if (every <= 0) {
                every = 1;
            }
            if (loopCount % every == 0) {
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
     * @param task the task to add (skipped if name already exists)
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
     * @param name the task name to remove
     * @return the removed task or null if not found
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
