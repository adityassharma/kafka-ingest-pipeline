package io.github.adityassharma.kafka.management;

public class SourceStats {

    public final String name;
    public final String type;
    public volatile ComponentStatus status = ComponentStatus.STARTING;

    public SourceStats(String name, String type) {
        this.name = name;
        this.type = type;
    }
}
