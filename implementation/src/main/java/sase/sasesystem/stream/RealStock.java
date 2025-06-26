package sase.sasesystem.stream;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RealStock implements Event{

    int id;
    private String key;
    private long timestamp;
    private double open_price;
    private double peak_price;
    private double lowest_price;
    private double close_price;
    private double volume;
    private String eventType;

    private long actualArrivalTime;

    private static final Map<String, Integer> map;

    public RealStock(String key, long timestamp, double op, double pp, double lp, double cp, double volume, int id){
        this.id=id;
        this.key=key;
        this.timestamp=timestamp;
        this.open_price=op;
        this.peak_price=pp;
        this.lowest_price=lp;
        this.close_price=cp;
        this.volume=volume;
        this.actualArrivalTime = System.nanoTime();
        this.eventType="realStock";
    }

    public RealStock(String line, int id){
        this.id=id;
        String tokens[] = line.split(",");
        this.key = tokens[0];
        this.timestamp = Long.parseLong(tokens[1]);
        this.open_price = Double.parseDouble(tokens[2]);
        this.peak_price = Double.parseDouble(tokens[3]);
        this.lowest_price = Double.parseDouble(tokens[4]);
        this.close_price = Double.parseDouble(tokens[5]);
        this.volume = Double.parseDouble(tokens[6]);
        this.actualArrivalTime = System.nanoTime();
        this.eventType = "realStock";
    }


    @Override
    public int getAttributeByName(String attributeName) {
        System.out.println(attributeName);
        if(attributeName.equalsIgnoreCase("key"))
            return map.get(key);
        if(attributeName.equalsIgnoreCase("open_price"))
            return (int) this.open_price;
        if(attributeName.equalsIgnoreCase("peak_price"))
            return (int) this.peak_price;
        if(attributeName.equalsIgnoreCase("lowest_price"))
            return (int) this.lowest_price;
        if(attributeName.equalsIgnoreCase("volume"))
            return (int) this.volume;
        if(attributeName.equalsIgnoreCase("timestamp"))
            return (int) this.timestamp;
        return map.get(attributeName);
    }

    @Override
    public double getAttributeByNameDouble(String attributeName) {
        if(attributeName.equalsIgnoreCase("key"))
            return map.get(key);
        if(attributeName.equalsIgnoreCase("open_price"))
            return (int) this.open_price;
        if(attributeName.equalsIgnoreCase("peak_price"))
            return (int) this.peak_price;
        if(attributeName.equalsIgnoreCase("lowest_price"))
            return (int) this.lowest_price;
        if(attributeName.equalsIgnoreCase("volume"))
            return (int) this.volume;
        if(attributeName.equalsIgnoreCase("timestamp"))
            return (int) this.timestamp;
        return map.get(attributeName);
    }

    @Override
    public String getAttributeByNameString(String attributeName) {
        if(attributeName.equalsIgnoreCase("key"))
            return this.key;
        if(attributeName.equalsIgnoreCase("open_price"))
            return String.valueOf(this.open_price);
        if(attributeName.equalsIgnoreCase("peak_price"))
            return String.valueOf(this.peak_price);
        if(attributeName.equalsIgnoreCase("lowest_price"))
            return String.valueOf(this.lowest_price);
        if(attributeName.equalsIgnoreCase("volume"))
            return String.valueOf(this.volume);
        if(attributeName.equalsIgnoreCase("timestamp"))
            return String.valueOf(this.timestamp);
        return attributeName;
    }

    public long arrivalTime() {
        return actualArrivalTime;
    }

    @Override
    public int getAttributeValueType(String attributeName) {
        return 0;
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public void setId(int Id) {
        this.id=Id;
    }

    @Override
    public int getTimestamp() {
        return (int) timestamp;
    }

    @Override
    public String getEventType() {
        return key;
    }

    @Override
    public Object clone() {
        return null;
    }

    @Override
    public String prepareEvent() {
        return null;
    }

    @Override
    public String toString() {
        return "RealStock{" +
                "id=" + id +
                ", key='" + key + '\'' +
                ", timestamp=" + timestamp +
                ", open_price=" + open_price +
                ", peak_price=" + peak_price +
                ", lowest_price=" + lowest_price +
                ", close_price=" + close_price +
                ", volume=" + volume +
                ", eventType='" + eventType + '\'' +
                '}';
    }

    static {
        Map<String, Integer> aMap = new HashMap<>();
        aMap.put("AAPL",0);
        aMap.put("AMZN",1);
        aMap.put("FB",2);
        aMap.put("AVID",3);
        map = Collections.unmodifiableMap(aMap);
    }
}
