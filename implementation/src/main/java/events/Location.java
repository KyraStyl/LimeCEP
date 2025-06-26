package events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

import static utils.UsefulFunctions.castStrToDate;

public class Location extends ABCEvent{

    private long patientID;
    private Position position;

    public Location(String name, Date timestamp, int symbol) {
        super(name, timestamp, "Locations", "Locations",symbol);
    }


    public Location(String name, String timestamp, long patientID, Position position, int symbol){
        super(name, timestamp,"Locations", position.location, symbol);
        this.patientID = patientID;
        this.position = position;
    }

    public long getPatientID(){return this.patientID;}
    public Position getPosition(){return this.position;}

    @Override
    public String toString() {
        return "{\"Location\":{" +
                "\"pID\":" + patientID +
                ",\"position\":" + position.toString() +
                "}}";
    }


    public static class Position{
        @JsonProperty("location")
        private String location;
        @JsonProperty("dateEntered")
        private Date dateEntered;
        @JsonProperty("dateUpdate")
        private Date dateUpdate;
        @JsonProperty("totalActivations")
        private int totalActivations;
        @JsonProperty("recentActivations")
        private int recentActivations;

        //for deserialization
        public Position(){}

        public Position(String location, String dateEntered, String dateUpdate, int totalActivations, int recentActivations){
            this.location = location;
            this.dateEntered = castStrToDate(dateEntered);
            this.dateUpdate = castStrToDate(dateUpdate);
            this.totalActivations = totalActivations;
            this.recentActivations = recentActivations;
        }

        public Date getDateEntered() {
            return dateEntered;
        }

        public Date getDateUpdate() {
            return dateUpdate;
        }

        public int getTotalActivations() {
            return totalActivations;
        }

        public int getRecentActivations() {
            return recentActivations;
        }

        @Override
        public String toString() {
            return "{\"location\":" +"\""+ location + '\"' +
                    ",\"dateEntered\":" +"\"" + dateEntered.toString() + '\"' +
                    ",\"dateUpdate\":" +"\"" + dateUpdate.toString() + '\"' +
                    ",\"totalActivations\":" +"\"" + totalActivations + '\"' +
                    ",\"recentActivations\":" +"\"" + recentActivations + '\"' +
                    "}";
        }
    }
}
