package events;


import handlers.KafkaMessageHandler;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

public class Source<T> {

    private String name;
    private HashMap<String, Class> eventTypes;
    private KafkaMessageHandler handler;
    private Long estimated;

    public Source(String name, KafkaMessageHandler handler){
        this.name = name;
        this.handler = handler;
        this.eventTypes = new HashMap<>();
    }

    public Source(String name, KafkaMessageHandler handler, Long estimated){
        this(name,handler);
        this.estimated = estimated;
    }

    public void addType(String type, Class classname){
        this.eventTypes.put(type,classname);
    }

    public String name() {
        return name;
    }

    public ArrayList<String> getEventTypes() {
        return new ArrayList<>(eventTypes.keySet());
    }

    public Class getEventType(String type){
        return eventTypes.get(type);
    }

    public boolean hasEventType(String type){ return eventTypes.containsKey(type);}

    public ArrayList<ABCEvent> processMessage(JSONObject message){
        return handler.processMessage(message);

    }

    public Long estimated() {
        return this.estimated;
    }
}
