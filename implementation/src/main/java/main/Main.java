package main;

import org.apache.kafka.common.protocol.types.Field;
import sase.sasesystem.UI.CommandLineUI;
import events.*;
import examples.ABCExample;
import examples.ExampleCEP;
import kafka.*;
import kafka.consumer.CustomKafkaListener;
import managers.EventManager;
import net.sourceforge.jeval.EvaluationException;
import cep.CEPQuery;
import stats.StatisticManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static utils.ApplicationConstant.*;


public class Main {

    //public static HashMap<String, TreeSet<ABCEvent>> acceptedEventsHashlist = new HashMap<>();
    public static ArrayList<String> dataTypes = new ArrayList<>();
    public static HashMap<String, EventManager> evManagers = new HashMap<>();
    public static HashMap<String, HashMap<String, TreeSet<ABCEvent>>> STS = new HashMap<>();
    public static HashMap<String, CEPQuery> queries = new HashMap<>();
    public static HashMap<String, ArrayList<String>> qte = new HashMap<>();
    public static HashMap<String, ArrayList<String>> etq = new HashMap<>();
    public static StatisticManager generalStats;
    public static HashMap<String, String> typeSourceMapping;
    public static HashMap<String, CustomKafkaListener> consumers;
    public static HashMap<String, Thread> threadsConsumers;
    public static final String defaultBootStrapServer = KAFKA_LOCAL_SERVER_CONFIG;
    public static System.Logger.Level info = System.Logger.Level.INFO;
    public static System.Logger.Level error = System.Logger.Level.ERROR;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

//        String engine = "LIMECEP";
//        String engine = "SASEXT";
        String engine = "SASE";
//        Boolean isOOO = true;
        Boolean isOOO = false;
//        String oooType = "full";
//        String oooType = "partial";
        String oooType = "ooo";
//        Boolean isSimple = true;
        Boolean isSimple = false;
//        String pattern = "abc";
//        String pattern = "ab+c";
        String pattern = "a+b+c";
//        String pattern = "multiple";
//
//        String policy = "Skip-till-next-match";
        String policy = "Skip-till-any-match";

        Boolean withCorrection = true;

        if (engine.equalsIgnoreCase("LIMECEP")){
            KafkaAdminClient kafkaAdminClient = new KafkaAdminClient(defaultBootStrapServer);
            System.out.println(kafkaAdminClient.verifyConnection());
        }

//        ExampleCEP ex = new LCExample();
//        ex.initializeExample();
//        typeSourceMapping = ((LCExample) ex).getEventTypeSourceMapping();

        ExampleCEP ex = new ABCExample();
        ex.initializeExample();
        typeSourceMapping = ((ABCExample) ex).getEventTypeSourceMapping();

        ArrayList<Source> sources = ex.getSources();
        HashMap<String,Long> estimatedArrivalTime = ex.getEstimated();

        ArrayList<String> queriesToRun = new ArrayList<>();
//        queriesToRun.add("src/main/resources/abc.query");
//        queriesToRun.add("src/main/resources/ab+c.query");
//        queriesToRun.add("src/main/resources/a+b+c.query");
//        queriesToRun.add("src/main/resources/bca.query");
        queriesToRun.add("src/main/resources/c+a+b.query");
        if(pattern.equalsIgnoreCase("abc"))
            queriesToRun.add("src/main/resources/abc.query");
        if(pattern.equalsIgnoreCase("ab+c"))
            queriesToRun.add("src/main/resources/ab+c.query");
        if(pattern.equalsIgnoreCase("a+b+c"))
            queriesToRun.add("src/main/resources/a+b+c.query");


        generalStats = new StatisticManager(0.6,0.2,0.2,2.5);
        generalStats.initializeManager(ex.getListofTypes());
        generalStats.setEstimated(estimatedArrivalTime);

        initialization(queriesToRun,sources,engine,isOOO,pattern,policy,oooType,isSimple, withCorrection);

        consumers = new HashMap<>();
        threadsConsumers = new HashMap<>();

        initializeConsumers(sources,engine);

        startConsumerThreads();

    }

    public static void initialization(ArrayList<String> queriesToRun, ArrayList<Source> sources, String engine, Boolean isOOO, String query, String policy, String oooType, Boolean isSimple, Boolean withCorrection) {
        System.out.println("Starting engine: "+engine);
        if (engine.equalsIgnoreCase("LIMECEP")) {
            initializeLIMECEP(queriesToRun, sources, policy, withCorrection);
        }else if (engine.equalsIgnoreCase("SASEXT")) {
            initializeSASE(true,isOOO, oooType, isSimple, query, policy);
        } else if (engine.equalsIgnoreCase("SASE")) {
            initializeSASE(false,isOOO, oooType, isSimple, query, policy);
        }else{
            //logger.log(error,"WRONG ENGINE");
            System.out.println("Wrong engine");
            System.exit(100);
        }
    }

    public static void initializeLIMECEP(ArrayList<String> queriesToRun, ArrayList<Source> sources, String policy, Boolean withCorrection){
        int i=0;
        for(String q: queriesToRun){
            //event manager
            String em_id = "Q"+(i+1);
            EventManager<ABCEvent> em = new EventManager<>(sources, q,em_id, policy, withCorrection);
            em.setStatManager(generalStats);
            evManagers.put(em_id,em);
            em.initializeManager();

            //query list
            queries.put(em_id,em.getQuery());
            System.out.println(em_id+": "+em.getQuery().toString());
            for (String tptrans : em.getQuery().getTransitions().keySet())
                System.out.println(em_id+": tptrans = "+tptrans+" TRANSITION: "+em.getQuery().getTransitions().get(tptrans).toString());
            System.out.println(em_id+": first_state = "+em.getFirstState());
            System.out.println(em_id+": last_state = "+em.getLastState());
            System.out.println();

            //qte
            ArrayList<String> etypes = em.getQueryTypes();
            qte.put(em_id,etypes);
            //etq
            for(String t: etypes){
                if(!etq.containsKey(t)){
                    etq.put(t, new ArrayList<>());
                }
                etq.get(t).add(em_id);
            }

            //for next query
            i++;
        }
    }


    private static void initializeSASE(Boolean sasext, Boolean ooo, String oootype, Boolean simple, String query, String policy) {
        String engine = sasext?"core":"sasesystem";
        String dataset = "src/main/resources/dataset";
        dataset = ooo?dataset+"-"+oootype:dataset+"-test";
        dataset = simple?dataset+"-simple":dataset;
        dataset+= "-sase.stream";
        String policyShort = policy.equalsIgnoreCase("Skip-till-next-match")?"stnm":"stam";
        String[] generatedArgs = {
                "-q", "src/main/resources/sase-"+query+"-"+policyShort+".query", // Query file
                "-i", dataset,     // Input stream file path
                "-t", "stock",                  // Event type
                "-e", engine,                   // Engine type (sase or core)
                "-w",
                "-o", "src/main/resources/output-"+engine+"-"+query+"-"+policyShort+".txt"       // Output file for results
        };

        System.out.println("Generated arguments for engine: "+engine);


        // Call CommandLineUI with generated arguments
        try {
            CommandLineUI.main(generatedArgs);
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        } catch (EvaluationException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.exit(0);
    }


    public static void initializeConsumers(ArrayList<Source> sources, String engine){

        for(Source source:sources){
            HashMap<String, TreeSet<ABCEvent>> sourceMap = new HashMap<>();
            STS.put(source.name(),sourceMap);
            for(String type: (ArrayList<String>)source.getEventTypes()){
                TreeSet<ABCEvent> tree = new TreeSet<>(new TimestampComparator());
                STS.get(source.name()).put(type,tree);
            }

            consumers.put(source.name(), new CustomKafkaListener(source.name(), defaultBootStrapServer, STS.get(source.name()), source, engine));
            threadsConsumers.put(source.name(), new Thread(consumers.get(source.name())));
        }
    }

    public static void startConsumerThreads(){
        System.out.println("Starting consumers");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumers.values().forEach(CustomKafkaListener::shutdown);
            threadsConsumers.values().forEach(thread -> {
                try {
                    thread.join();  // Wait for all threads to finish
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    //logger.log(error,"Failed to stop consumer threads gracefully");
                    System.out.println("Failed to stop consumer threads gracefully");
                }
            });
        }));

        threadsConsumers.values().forEach(Thread::start);
    }

    public static void updateManagers(ABCEvent e){
        generalStats.updateStats(e);
        if (!etq.containsKey(e.getType())) return;
        for(String query: etq.get(e.getType())){
            evManagers.get(query).acceptEvent(e.getSource(),e);
        }
    }

    public static void runSASEonce(ABCEvent e) {

    }

    public static void runSASEXTonce(ABCEvent e) {
    }

    public static void printRMProfiling(){
        System.out.println("-----------------------------------------");
        for(EventManager em : evManagers.values()){
            em.printRMprofiling();
        }
    }

    public static void printSMProfiling() {
        generalStats.printProfiling();
    }
}