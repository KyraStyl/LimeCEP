package sase.sasesystem.stream;

/**
 * This class represents a kind of event.
 * @author kyrastyl
 *
 */
public class CheckEvent implements Event{
    /**
     * Event id
     */
    int id;

    /**
     * Event timestamp
     */
    int timestamp;

    /**
     * Event type
     */
    String eventType;

    /**
     * source
     */
    String src;

    /**
     * destination
     */
    String dst;

    long actualArrivalTime;

    /**
     * Constructor
     */
    public CheckEvent(int i, int ts, String src, String dst){
        id = i;
        timestamp = ts;
        this.src = src;
        this.dst = dst;
        this.actualArrivalTime = System.nanoTime();
    }

    public CheckEvent(String line){
        String tokens[] = line.split(", ");
        this.id = Integer.parseInt(tokens[0]);
        this.timestamp = Integer.parseInt(tokens[1]);
        this.src = tokens[2];
        this.dst = tokens[3];
        this.actualArrivalTime = System.nanoTime();
        this.eventType = "check";
    }

    /**
     * @return the cloned event
     */
    public Object clone(){
        CheckEvent o = null;
        try {
            o = (CheckEvent) super.clone();
        } catch (CloneNotSupportedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return o;
    }

    @Override
    public long arrivalTime() {
        return actualArrivalTime;
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return the timestamp
     */
    public int getTimestamp() {
        return timestamp;
    }

    @Override
    public String getEventType() {
        return this.eventType;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return the event src
     */
    public String getEventSrc() {
        return src;
    }

    /**
     * @return the event dst
     */
    public String getEventDst() {
        return dst;
    }

    /**
     * @param src the src to set
     */
    public void setEventSrc(String src) {
        this.src = src;
    }

    /**
     * @param dst the src to set
     */
    public void setEventDst(String dst) {
        this.dst = dst;
    }


    public String toString(){
        return "ID="+ id + "\tTimestamp=" + timestamp
                + "\tSouce=" + src + "\tDestination=" + dst;
    }

    /* (non-Javadoc)
     * @see edu.umass.cs.sasesystem.mvc.model.Event#getAttributeByName(java.lang.String)
     */
    public int getAttributeByName(String attributeName) {

        if(attributeName.equalsIgnoreCase("src"))
            return Integer.parseInt(src.replaceAll("[^0-9]", ""));
            //return 'A'-src.charAt(0);
        if(attributeName.equalsIgnoreCase("dst"))
            return Integer.parseInt(dst.replaceAll("[^0-9]", ""));
            //return 'A'-dst.charAt(0);
        if(attributeName.equalsIgnoreCase("id"))
            return this.id;
        if(attributeName.equalsIgnoreCase("timestamp"))
            return this.timestamp;

        return -1;
    }

    /* (non-Javadoc)
     * @see edu.umass.cs.sasesystem.mvc.model.Event#getAttributeByNameString(java.lang.String)
     */
    public String getAttributeByNameString(String attributeName) {
        // TODO Auto-generated method stub
        return null;
    }
    /* (non-Javadoc)
     * @see edu.umass.cs.sasesystem.mvc.model.Event#getAttributeValueType(java.lang.String)
     */
    public int getAttributeValueType(String attributeName) {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see edu.umass.cs.sasesystem.mvc.model.Event#getAttributeByNameDouble(java.lang.String)
     */
    public double getAttributeByNameDouble(String attributeName) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String prepareEvent() {
        String s = "";
        s+=timestamp+" ";
        s+=id+" ";
        s+=src+" ";
        s+=dst+" ";
        return s;
    }
}
