package events;

import java.util.Date;

public class ClusterEvent extends ABCEvent {

    private final int instanceIndex;
    private final int clusterEventType;

    public ClusterEvent(
            String name,
            Date timestamp,
            String source,
            String eventType,
            int symbol,
            int instanceIndex,
            int clusterEventType
    ) {
        super(name, timestamp, source, eventType, symbol);   // ← IMPORTANT
        this.instanceIndex = instanceIndex;
        this.clusterEventType = clusterEventType;
    }

    public int getInstanceIndex() {
        return instanceIndex;
    }

    public int getClusterEventType() {
        return clusterEventType;
    }

    @Override
    public String toString() {
        return "ClusterEvent{" +
                "id=" + getId() +
                ", name='" + getName() + '\'' +
                ", ts=" + getTimestampDate() +
                ", type='" + getType() + '\'' +
                ", instanceIndex=" + instanceIndex +
                ", clusterEventType=" + clusterEventType +
                '}';
    }
}
