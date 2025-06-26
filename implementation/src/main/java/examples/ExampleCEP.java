package examples;

import events.Source;

import java.util.ArrayList;
import java.util.HashMap;

public interface ExampleCEP {

    void initializeExample();

    ArrayList<Source> getSources();
    ArrayList<String> getListofTypes();
    HashMap<String,Long> getEstimated();
}
