package io.velo.command;

import com.moilioncircle.redis.replicator.Replicator;
import com.moilioncircle.redis.replicator.event.Event;
import com.moilioncircle.redis.replicator.event.EventListener;
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair;

public class MyRDBVisitorEventListener implements EventListener {
    private final LGroup lGroup;

    int keyCount;

    public MyRDBVisitorEventListener(LGroup lGroup) {
        this.lGroup = lGroup;
    }

    // todo, save to velo local persist
    @Override
    public void onEvent(Replicator replicator, Event event) {
        System.out.println(event);

        if (event instanceof KeyValuePair<?, ?>) {
            keyCount++;

//            if (event instanceof KeyStringValueString kv) {
//            }
        }
    }
}
