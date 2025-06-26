package sase.sasesystem.stream;

public class Pair {

    private Event e;
    private int pos;

    public Pair(){}
    public Pair(Event e){this.e = e;}
    public Pair(Event e, int pos){
        this.e = e;
        this.pos = pos;
    }

    public void setEvent(Event e) {
        this.e = e;
    }

    public void setPosition(int pos) {
        this.pos = pos;
    }

    public boolean changePositionBY(int pos) { this.pos -= pos; return this.pos>=0;}

    public Event event() {
        return e;
    }

    public int position() {
        return pos;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "e= (" + e.getId()+", " +e.getTimestamp()+" )"+
                ", pos=" + pos +
                '}';
    }
}
