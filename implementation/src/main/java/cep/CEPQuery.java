package cep;

import events.Source;
import main.Main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CEPQuery {
    private String pattern;
    private LogicalCondition whereClause;  // Logical representation of WHERE clause
    private String within;
    private long timeWindow; //window in milliseconds
    private String firstState;
    private String lastState;
    private HashMap<String, CEPTransition> transitions;
    private ArrayList<Source> sources;

    public CEPQuery(String pattern, LogicalCondition whereClause, String within) {
        this.pattern = pattern;
        this.whereClause = whereClause;
        this.within = within;
        this.transitions = new HashMap<>();
        computeStates();
        computeTransitions();
        computeTimeWindow();
    }

    private void computeStates(){
        String[] parts = this.pattern.split(",\\s*");
        this.firstState = parts[0].split("\\+|\\s")[0];
        this.lastState = parts[parts.length-1].split("\\s")[0];
    }

    private void computeTransitions(){
        String[] parts = this.pattern.split(",\\s*");
        for(int i=0;i<parts.length;i++){
            boolean iskleene = parts[i].contains("+");
            String type = parts[i].split("\\+|\\s")[0];
            transitions.put(type, new CEPTransition("S"+i, "S"+(i+1), "", type, iskleene));
            transitions.get(type).setSource(Main.typeSourceMapping.get(type));
        }
    }

    private void computeTimeWindow(){
        Pattern pattern = Pattern.compile("(\\d+)\\s*(\\w+)");
        Matcher matcher = pattern.matcher(this.within);
        if (matcher.find()) {
            // Extract the number and unit
            long value = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2).toLowerCase().trim();

            // Convert the time unit to seconds
            switch (unit) {
                case "ms":
                case "millisecond":
                case "milliseconds":
                    this.timeWindow = value;
                    return;
                case "sec":
                case "second":
                case "seconds":
                    this.timeWindow = value * 1000;
                    return;
                case "min":
                case "minute":
                case "minutes":
                    this.timeWindow = value * 60 * 1000;
                    return;
                case "h":
                case "hr":
                case "hrs":
                case "hour":
                case "hours":
                    this.timeWindow = value * 60 * 60 * 1000;
                    return;
                case "d":
                case "day":
                case "days":
                    this.timeWindow = value * 24 * 60 * 60  * 1000;
                    return;
                default:
                    throw new IllegalArgumentException("Unsupported time unit: " + unit);
            }
        }

        // If no match, 0
        this.timeWindow = 0;
    }

    public String getPattern() {
        return pattern;
    }

    public LogicalCondition getWhereClause() {
        return whereClause;
    }

    public String getWithin() {
        return within;
    }

    public long getTimeWindow() {
        return timeWindow;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PATTERN: ").append(pattern).append("\n");
        if (whereClause != null) {
            sb.append("WHERE: ").append(whereClause).append("\n");
        }
        sb.append("WITHIN: ").append(within).append("\n");
        return sb.toString();
    }

    public HashMap<String, CEPTransition> getTransitions() {
        return transitions;
    }

    public void setSources(ArrayList<Source> sources) {
        this.sources = sources;
    }

    public String lastState() {
        return lastState;
    }

    public String firstState(){
        return firstState;
    }
}

