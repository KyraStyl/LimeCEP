package sase.parser;

import com.beust.jcommander.Parameter;

public class Params {

    @Parameter(names = {"-q", "--query"}, description = "query path", required = true)
    String query = null;

    @Parameter(names = {"-c","--conf"}, description = "stream conf file")
    String conf = null;

    @Parameter(names = {"-i", "--inputStream"}, description = "input stream file path", required = true)
    String input = null;

    @Parameter(names = {"-t", "--eventtype"}, description = "type of events", required = true)
    String eventtype = "stock";

    @Parameter(names = {"-e", "--engine"}, description = "engine type: sase or cet")
    String engine = "null";

    @Parameter(names = {"-p","--parallelism"}, description = "parallelism for cet")
    int p = 4;

    @Parameter(names = {"-w", "-write"}, description = "write graph to directory")
    boolean isWrite = false;

    @Parameter(names = {"-o","--out"}, description = "output file for results")
    String outFile = "output-results";

    @Parameter(names = {"-h","--help"}, help = true)
    public boolean help;
}