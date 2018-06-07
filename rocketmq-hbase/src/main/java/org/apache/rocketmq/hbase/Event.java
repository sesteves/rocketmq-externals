package org.apache.rocketmq.hbase;

public interface Event {
    enum EventType {
        PUT, DELETE
    }

    EventType getType();
}
