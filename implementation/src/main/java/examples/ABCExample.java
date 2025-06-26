package examples;

import events.KeyValueEvent;
import events.Source;
import handlers.ABCMessageHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static utils.UsefulFunctions.secondsToMillis;

public class ABCExample implements ExampleCEP{

    private ArrayList<Source> sources;
    private HashMap<String, Long> estimated;

    @Override
    public void initializeExample() {
        initializeSources();
        initializeEstimated();
    }

    private void initializeSources() {
        this.sources = new ArrayList<>();
        Source abc = new Source("Abc", new ABCMessageHandler(), secondsToMillis(10));
        abc.addType("a", KeyValueEvent.class);
        abc.addType("b", KeyValueEvent.class);
        abc.addType("c", KeyValueEvent.class);

        this.sources.add(abc);
    }

    private void initializeEstimated(){
        this.estimated = new HashMap<>();
        for(Source s: this.sources){
            for(Object type: s.getEventTypes()){
                estimated.put((String)type,s.estimated());
            }
        }
    }

    @Override
    public ArrayList<Source> getSources() {
        return sources;
    }

    @Override
    public ArrayList<String> getListofTypes() {
        HashSet<String> names = new HashSet<>();
        for(Source s: sources){
            names.addAll(s.getEventTypes());
        }
        return new ArrayList<>(names);
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
