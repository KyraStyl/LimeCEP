package events;

import sase.sasesystem.stream.Event;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Date;
import java.util.Objects;

import static utils.UsefulFunctions.*;

public class ABCEvent implements Event {
    private Long id;
    private String name;
    private Date timestamp;
    private static Long idcounter = 0L;
    private String source;
    private String type;
    private long ingestionTime;
    private int symbol;
    private boolean tempEvent = false;

    @JsonCreator
    public ABCEvent(@JsonProperty("name") String name, @JsonProperty("timestamp") Date timestamp, @JsonProperty("source") String source, String type, int symbol){
        this.ingestionTime = System.nanoTime();
        this.type = type;
        this.id = idcounter;
        this.name = name;
        this.timestamp = timestamp;
        this.source = source;
        this.symbol = symbol;
        if (!tempEvent)
            idcounter++;
    }

    public ABCEvent(KeyValueEvent e){
        this(e.getName(),e.getTimestampDate(),e.getSource(),e.getType(),e.getSymbol());
    }

    public ABCEvent(@JsonProperty("name") String name, @JsonProperty("timestamp") Date timestamp, @JsonProperty("source") String source, String type, int symbol, boolean tempEvent){
        this.ingestionTime = System.nanoTime();
        this.type = type;
        this.id = idcounter;
        this.name = name;
        this.timestamp = timestamp;
        this.source = source;
        this.symbol = symbol;
        this.tempEvent = tempEvent;
        if (!this.tempEvent)
            idcounter++;
    }

    public ABCEvent(@JsonProperty("name") String name, @JsonProperty("timestamp") String timestamp, @JsonProperty("source") String source, String type, int symbol){
        this.ingestionTime = System.currentTimeMillis();
        this.type = type;
        this.id = idcounter;
        this.name = name;
        this.timestamp = castStrToDate(timestamp);
        this.source = source;
        this.symbol = symbol;
        if (!tempEvent)
            idcounter++;
    }

    public ABCEvent(@JsonProperty("name") String name, @JsonProperty("timestamp") String timestamp, String type, int symbol){
        this(name,castStrToDate(timestamp), "none", type, symbol);
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setSymbol(int symbol) {
        this.symbol = symbol;
    }

    public int getSymbol() {
        return symbol;
    }

    public Date getTimestampDate(){ return timestamp; }

    public int getTimestamp(){ return (int) timestamp.getTime(); }

    @Override
    public String getEventType() {
        return this.type;
    }

    @Override
    public Object clone() {
        return this;
    }

    @Override
    public String prepareEvent() {
        return this.toString();
    }

    @Override
    public long arrivalTime() {
        return 0;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int getAttributeByName(String attributeName) {
        switch (attributeName){
            case "symbol": return this.symbol;
            case "id": return this.getId();
            default: return 0;
        }
    }

    @Override
    public double getAttributeByNameDouble(String attributeName) {
        return 0;
    }

    @Override
    public String getAttributeByNameString(String attributeName) {
        return "";
    }

    @Override
    public int getAttributeValueType(String attributeName) {
        return 0;
    }

    public Long getIdLong() {
        return id;
    }

    public int getId() {
        return Math.toIntExact(id);
    }

    @Override
    public void setId(int Id) {

    }

    public void setId(Long id) {
        this.id = id;
    }

    public long getIngestionTime() {
        return ingestionTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "ABCEvent{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ABCEvent)) return false;
        ABCEvent abcEvent = (ABCEvent) o;
        return Objects.equals(this.getId(), abcEvent.getId()) && Objects.equals(getName(), abcEvent.getName()) && Objects.equals(getTimestampDate(), abcEvent.getTimestampDate()) && Objects.equals(getSource(), abcEvent.getSource()) && Objects.equals(getType(), abcEvent.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getId(), getName(), getTimestampDate(), getSource(), getType());
    }
    
    public int compareTo(ABCEvent other) {
        return this.timestamp.compareTo(other.timestamp);
    }
}