package sase.parser;

import com.beust.jcommander.JCommander;
import sase.sasesystem.UI.CommandLineUI;
import sase.sasesystem.engine.ConfigFlags;

import java.util.Locale;

public final class InputParamParser {

    private static Params params = new Params();

    public static void validateParams(String args[]){
        JCommander.newBuilder().addObject(params).build().parse(args);
        return;
    }

    public static void readParams(){
//        String parrent_dir = Paths.get(System.getProperty("user.dir")).getParent()+"/";
        String parrent_dir = "";

        CommandLineUI.nfaFileLocation = parrent_dir+params.query;
        ConfigFlags.queryFile = CommandLineUI.nfaFileLocation;
        CommandLineUI.inputFile = parrent_dir+params.input;
        ConfigFlags.inputFile = CommandLineUI.inputFile;
        ConfigFlags.engine = params.engine;

        if(params.conf!=null){
            CommandLineUI.streamConfigFile = parrent_dir+params.conf;
        }

        ConfigFlags.printResults = params.isWrite;
        ConfigFlags.outFile = parrent_dir+params.outFile;
        ConfigFlags.parallelism = params.p;

        CommandLineUI.eventtype = params.eventtype.equalsIgnoreCase("kite")?"check":params.eventtype;
        ConfigFlags.eventtype = CommandLineUI.eventtype.toLowerCase(Locale.ROOT);

        if(params.help){
            System.out.println(getHelp());
            System.exit(400);
        }
    }

    public static String getHelp() {
        return "This program can't run without some input parameters!\n"+
                "Required parameters:\n" +
                "    -q, --query          Path of query file\n" +
                "    -i, --inputStream    Path of Input Stream file\n" +
                "    -t, --eventtype      Type of events: stock or check\n" +
                "Optional parameters:\n" +
                "    -e, --engine         The engine to run: sase or cet (default sase)\n" +
                "    -p, --parallelism    Degree of parallelism for cet\n" +
                "    -w, --write          Whether to write or not the output\n" +
                "    -o, --output         The output file for results\n" +
                "    -c, --conf           The stream configuration file\n" +
                "Try running again providing the appropriate input parameters!\n";

    }

}





