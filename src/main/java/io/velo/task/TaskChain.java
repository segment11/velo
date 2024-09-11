package io.velo.task;

import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class TaskChain {
    private static final Logger log = LoggerFactory.getLogger(TaskChain.class);

    // need not thread safe
    final ArrayList<ITask> list = new ArrayList<>();

    @VisibleForTesting
    ArrayList<ITask> getList() {
        return list;
    }

    @Override
    public String toString() {
        return "TaskChain{" +
                "list.size=" + list.size() +
                ", list.names=" + list.stream().map(ITask::name).reduce((a, b) -> a + "," + b).orElse("") +
                '}';
    }

    public void doTask(int loopCount) {
        for (var t : list) {
            if (loopCount % t.executeOnceAfterLoopCount() == 0) {
                t.setLoopCount(loopCount);

                try {
                    t.run();
                } catch (Exception e) {
                    log.error("Task error, name=" + t.name(), e);
                }
            }
        }
    }

    public void add(ITask task) {
        for (ITask t : list) {
            if (t.name().equals(task.name())) {
                return;
            }
        }

        list.add(task);
    }

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
