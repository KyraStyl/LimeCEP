package cep;

public class CEPTransition {

    private String src;
    private String dest;

    private String eventType;
    private String source;
    private LogicalCondition p;

    private boolean isKleene;

    public CEPTransition(){}

    public CEPTransition(String src, String dst, String s, String et, boolean isKleene){
        this.src = src;
        this.dest = dst;
        this.source = s;
        this.eventType = et;
        this.isKleene = isKleene;
    }

    public CEPTransition(String src, String dst, String s, String et, boolean isKleene, LogicalCondition p){
        this.src = src;
        this.dest = dst;
        this.source = s;
        this.eventType = et;
        this.isKleene = isKleene;
        this.p = p;
    }

    public String getSrc() {
        return src;
    }

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getEventType() {
        return eventType;
    }

    public LogicalCondition getP() {
        return p;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public void setP(LogicalCondition p) {
        this.p = p;
    }

    public boolean isKleene() {
        return isKleene;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSource() {
        return source;
    }

    @Override
    public String toString() {
        return "Transition{" +
                "from src='" + src + '\'' +
                ", to dest='" + dest + '\'' +
                ", requiring eventType='" + eventType + '\'' +
                ", with condition p=" + p +
                ", isKleene=" + isKleene +
                '}';
    }

}
