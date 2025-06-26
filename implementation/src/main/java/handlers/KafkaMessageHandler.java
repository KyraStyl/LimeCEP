package handlers;

import events.ABCEvent;
import org.json.JSONObject;

import java.util.ArrayList;

public interface KafkaMessageHandler {

    ArrayList<ABCEvent> processMessage(JSONObject message);
}
