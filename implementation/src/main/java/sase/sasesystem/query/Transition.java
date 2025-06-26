package sase.sasesystem.query;

public class Transition {
    private State src;
    private State dest;

    private String eventType;
    private PredicateOptimized p;

    public Transition(){}
    public Transition(State src, State dst, String et, PredicateOptimized p){
        this.src = src;
        this.dest = dst;
        this.eventType = et;
        this.p = p;
    }

    public State getSrc() {
        return src;
    }

    public State getDest() {
        return dest;
    }

    public void setDest(State dest) {
        this.dest = dest;
    }

    public void setSrc(State src) {
        this.src = src;
    }

    public String getEventType() {
        return eventType;
    }

    public PredicateOptimized getP() {
        return p;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setP(PredicateOptimized p) {
        this.p = p;
    }

    @Override
    public String toString() {
        return "This is a transition" +
                " from src= " + src +
                ", to dest=" + dest +
                ", accepting events of type '" + eventType + '\'' +
                ", by evaluating the predicate: " + p;
    }
}
