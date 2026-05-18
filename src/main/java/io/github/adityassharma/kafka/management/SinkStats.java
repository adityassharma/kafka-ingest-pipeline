package io.github.adityassharma.kafka.management;

import java.util.concurrent.ConcurrentHashMap;

public class SinkStats {

    public final String name;
    public final String type;
    public volatile ComponentStatus status = ComponentStatus.STARTING;
    public final ConcurrentHashMap<String, Long> lagByPartition = new ConcurrentHashMap<>();

    public SinkStats(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public void updateLag(String topicPartition, long lag) {
        lagByPartition.put(topicPartition, lag);
    }
}
