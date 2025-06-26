package handlers;

import events.ABCEvent;
import events.KeyValueEvent;
import org.json.JSONObject;

import java.util.ArrayList;

public class ABCMessageHandler implements KafkaMessageHandler{

    private int counter = 0;
    private String source = "Abc";

    @Override
    public ArrayList<ABCEvent> processMessage(JSONObject input) {
        int price = input.getInt("price");
        String symbol = input.getString("symbol");
        double volume = input.getDouble("volume");
        String ts = input.getString("timestamp");
        int id = input.getInt("id");

        KeyValueEvent<Double> e = new KeyValueEvent<>(symbol+id,ts,source,symbol,"volume",volume,price);
        ArrayList<ABCEvent> toreturn = new ArrayList<>();
        toreturn.add(e);

        counter++;

        return toreturn;
    }
}
