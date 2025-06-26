package utils;

import sase.sasesystem.query.State;
import sase.sasesystem.query.Transition;

import java.util.ArrayList;
import java.util.HashMap;

public class Configs {

    private boolean printResults;
    private String nfaFileLocation;
    private String outputFile;
    private boolean useSharing;
    private int windowLength;
    private State[] states;
    private String first_state;
    private String last_state;
    private HashMap<String, Boolean> isKleene;
    private HashMap<String, Transition> transitions;
    private ArrayList<String> statetypes;
    private String policy;

    public Configs(){
        initialize();
    }

    public void initialize(){
        printResults = true;
        nfaFileLocation = "";
        outputFile = "";
        useSharing = false;
        windowLength = 0;
        states = new State[0];
        isKleene = new HashMap<>();
        transitions = new HashMap<>();
        first_state = "";
        last_state = "";
        statetypes = new ArrayList<>();
        policy = "Skip-till-next-match";
    }

    public void setIsKleene(HashMap<String, Boolean> isKleene) {
        this.isKleene = isKleene;
    }
    public void setKleeneState(String key, boolean isk){
        this.isKleene.put(key,isk);
    }
    public boolean containsTypeInKleene(String type){
        return this.isKleene.containsKey(type);
    }

    public void setFirst_state(String first_state) {
        this.first_state = first_state;
    }

    public void setLast_state(String last_state) {
        this.last_state = last_state;
    }

    public void setPolicy(String policy) {
        this.policy = policy;
    }

    public void setNfaFileLocation(String nfaFileLocation) {
        this.nfaFileLocation = nfaFileLocation;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public void setPrintResults(boolean printResults) {
        this.printResults = printResults;
    }

    public void setStates(State[] states) {
        this.states = states;
    }

    public void setStatetypes(String pattern){
        String[] ss  = pattern.split(", ");

        for(int i=0;i<ss.length;i++){
            boolean isKl = false;
            if(ss[i].contains("+")) isKl = true;
            ss[i] = ss[i].replace("+","").strip().split(" ")[0];
            statetypes.add(ss[i]);
            isKleene.put(ss[i],isKl);
        }

        setFirst_state(statetypes.get(0));
        setLast_state(statetypes.get(statetypes.size()-1));
    }

    public void setUseSharing(boolean useSharing) {
        this.useSharing = useSharing;
    }

    public void setWindowLength(int windowLength) {
        this.windowLength = windowLength;
    }

    public void setTransitions(HashMap<String, Transition> trans) {
        this.transitions = trans;
    }

    public int windowLength() {
        return windowLength;
    }

    public State[] states() {
        return states;
    }

    public ArrayList<String> listOfStateTypes(){
        ArrayList<String> list = new ArrayList<>();
        for (State s : states()){
            list.add(s.getEventType());
        }
        return list;
    }

    public ArrayList<String> listofStateTypes(){
        return statetypes;
    }

    public String policy() {
        return policy;
    }

    public String first_state() {
        return first_state;
    }

    public String nfaFileLocation() {
        return nfaFileLocation;
    }

    public String last_state() {
        return last_state;
    }

    public String outputFile() {
        return outputFile;
    }

    public HashMap<String, Boolean> isKleene() {
        return isKleene;
    }

    public boolean isKleene(String key) {
        return isKleene.get(key);
    }

    public HashMap<String, Transition> transitions() {
        return transitions;
    }
}
