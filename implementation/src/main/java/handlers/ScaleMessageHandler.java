package handlers;

import events.ABCEvent;
import events.KeyValueEvent;
import org.json.JSONObject;
import java.util.ArrayList;

public class ScaleMessageHandler implements KafkaMessageHandler {

    private int counter = 0;
    private String source = "Scale";

    @Override
    public ArrayList<ABCEvent> processMessage(JSONObject input) {
        ArrayList<ABCEvent> toreturn = new ArrayList<>();

        JSONObject scale = input.getJSONObject("scale");
        String date = scale.getString("timestamp");
        double weight = scale.getDouble("weight");
        double height = scale.getDouble("height");
        double bmi = scale.getDouble("bmi");
        String bmicat = scale.getString("bmiCategory");

        KeyValueEvent<Double> weightE = new KeyValueEvent<>("weight_"+counter,date, this.source, "Weight", "weight", weight, 5);
        KeyValueEvent<Double> heightE = new KeyValueEvent<>("height_"+counter,date, this.source, "Height", "height", height, 6);
        KeyValueEvent<Double> bmiE = new KeyValueEvent<>("bmi_"+counter,date, this.source, "Bmi", "bmi", bmi, 7);
        KeyValueEvent<String> bmiCE = new KeyValueEvent<>("bmiCat_"+counter,date, this.source, "BmiCat", "bmicat", bmicat, 8);

        toreturn.add(weightE);
        toreturn.add(heightE);
        toreturn.add(bmiE);
        toreturn.add(bmiCE);

        counter++;

        return toreturn;
    }
}
