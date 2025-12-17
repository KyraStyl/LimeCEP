package examples;

import events.ClusterEvent;
import events.Source;
import handlers.ClusterMessageHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static utils.UsefulFunctions.secondsToMillis;

public class ClusterExample implements ExampleCEP {

    private ArrayList<Source> sources;
    private HashMap<String, Long> estimated;

    @Override
    public void initializeExample() {
        initializeSources();
        initializeEstimated();
    }

    private void initializeSources() {
        this.sources = new ArrayList<>();

        Source cluster = new Source("Cluster", new ClusterMessageHandler(), secondsToMillis(10));

        // Declare event types (letters mapped from cluster dataset)
        cluster.addType("a", ClusterEvent.class); // SUBMIT
        cluster.addType("b", ClusterEvent.class); // QUEUE
        cluster.addType("c", ClusterEvent.class); // ENABLE
        cluster.addType("d", ClusterEvent.class); // SCHEDULE
        cluster.addType("e", ClusterEvent.class); // EVICT
        cluster.addType("f", ClusterEvent.class); // FAIL
        cluster.addType("g", ClusterEvent.class); // FINISH
        cluster.addType("h", ClusterEvent.class); // KILL
        cluster.addType("i", ClusterEvent.class); // LOST
        cluster.addType("j", ClusterEvent.class); // UPDATE_PENDING
        cluster.addType("k", ClusterEvent.class); // UPDATE_RUNNING

        this.sources.add(cluster);
    }

    private void initializeEstimated() {
        this.estimated = new HashMap<>();

        // Correct: iterate over keySet() (event type names)
        for (Object type : sources.get(0).getEventTypes()) {
            estimated.put((String) type, sources.get(0).estimated());
        }
    }

    @Override
    public ArrayList<Source> getSources() {
        return sources;
    }

    @Override
    public ArrayList<String> getListofTypes() {
        // Correct: return list of event type names
        return new ArrayList<>(sources.get(0).getEventTypes());
    }

    @Override
    public HashMap<String, Long> getEstimated() {
        return estimated;
    }

    public HashMap<String, String> getEventTypeSourceMapping() {
        HashMap<String, String> eventTypeSourceMapping = new HashMap<>();

        for (Source source : sources) {
            ArrayList<String> eventTypes = source.getEventTypes();
            for (String eventType : eventTypes) {
                eventTypeSourceMapping.put(eventType, source.name());
            }
        }

        return eventTypeSourceMapping;
    }
}