package handlers;

import events.ClusterEvent;
import events.ABCEvent;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Date;

public class ClusterMessageHandler implements KafkaMessageHandler {

    private static final String SOURCE = "Cluster";

    private static final String[] TYPE_MAPPING = {
            "a", // SUBMIT
            "b", // QUEUE
            "c", // ENABLE
            "d", // SCHEDULE
            "e", // EVICT
            "f", // FAIL
            "g", // FINISH
            "h", // KILL
            "i", // LOST
            "j", // UPDATE_PENDING
            "k"  // UPDATE_RUNNING
    };

    @Override
    public ArrayList<ABCEvent> processMessage(JSONObject json) {

        // FIX: Incoming values are STRINGS → parse manually
        long timeMicros = Long.parseLong(json.getString("time"));
        int type = Integer.parseInt(json.getString("type"));
        int index = Integer.parseInt(json.getString("instance_index"));

        long timestampMs = timeMicros / 1000;

        String evtType = TYPE_MAPPING[type];

        ClusterEvent evt = new ClusterEvent(
                evtType + "_" + index + "_" + timeMicros,
                new Date(timestampMs),
                SOURCE,
                evtType,
                type,          // symbol
                index,
                type
        );

        ArrayList<ABCEvent> out = new ArrayList<>();
        out.add(evt);
        return out;
    }
}
