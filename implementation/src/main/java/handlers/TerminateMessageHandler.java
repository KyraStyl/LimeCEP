package handlers;

import events.ABCEvent;
import events.KeyValueEvent;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;

public class TerminateMessageHandler implements KafkaMessageHandler{
    @Override
    public ArrayList<ABCEvent> processMessage(JSONObject message) {
        Date now = new Date(System.currentTimeMillis());

        String formattedDateTime = utils.UsefulFunctions.formatter.format(now);

        ABCEvent e = new ABCEvent("terminate", formattedDateTime, "Terminate", "Terminate",-1);

        ArrayList<ABCEvent> toreturn = new ArrayList<>();
        toreturn.add(e);

        return toreturn;
    }
}
