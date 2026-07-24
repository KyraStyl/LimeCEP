package main;

import examples.ClusterExample;
import examples.LCExample;
import operator_exploration.GlobalStats;
import sase.sasesystem.UI.CommandLineUI;
import events.*;
import examples.ABCExample;
import examples.ExampleCEP;
import kafka.*;
import kafka.consumer.CustomKafkaListener;
import managers.EventManager;
import net.sourceforge.jeval.EvaluationException;
import cep.CEPQuery;
import semi_automated.PatternTrie;
import semi_automated.TrieManager;
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
    public static HashMap<String, HashMap<String, HashMap<String, Integer>>> STS_counts = new HashMap<>();
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
    public static TrieManager trieManager = new TrieManager();
    public static boolean suggestPatterns = true;
//    public static boolean suggestPatterns = false;
    public static String updateAlgorithm ="operator-explore";
//    public static String updateAlgorithm ="ic";

    // new imports for operator-based exploration
    public static List<ABCEvent> explorationBuffer = new ArrayList<>();
    public static int BATCH_SIZE = 1000;
    public static GlobalStats globalProbStats = new GlobalStats();
    public static int eventCounter = 0;
    public static boolean debug = false;
    public static int windowSize = 10000;
    public static String dataset = "5m";
    public static String operatorConfiguration = "all";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String engine = "LIMECEP";
//        String engine = "SASEXT";
//        String engine = "SASE";
//        Boolean isOOO = true;
        Boolean isOOO = false;
//        String oooType = "full";
//        String oooType = "partial";
        String oooType = "ooo";
//        Boolean isSimple = true;
        Boolean isSimple = false;
        String pattern = "abc";
//        String pattern = "ad";
//        String pattern = "ae";
 //       String pattern = "af";
//        String pattern = "ag";
//        String pattern = "clusterq1";
//        String pattern = "clusterq2";
//        String pattern = "af";
//        String pattern = "abc";
//        String pattern = "a+b+c";
//        String pattern = "multiple";
//
        String policy = "Skip-till-next-match";
//        String policy = "Skip-till-any-match";

        Boolean withCorrection = false;
        trieManager.setThetaFreq(0.2);
        trieManager.setThetaRare(0.001);

        if (engine.equalsIgnoreCase("LIMECEP")){
            KafkaAdminClient kafkaAdminClient = new KafkaAdminClient(defaultBootStrapServer);
            System.out.println(kafkaAdminClient.verifyConnection());
        }

//        ExampleCEP ex = new LCExample();
        ExampleCEP ex = new ABCExample();
//        ExampleCEP ex = new ClusterExample();
        // ---------------------------

        ex.initializeExample();

        if (ex instanceof LCExample){
            typeSourceMapping = ((LCExample) ex).getEventTypeSourceMapping();
        } else if (ex instanceof ABCExample) {
            typeSourceMapping = ((ABCExample) ex).getEventTypeSourceMapping();
        } else if (ex instanceof ClusterExample) {
            typeSourceMapping = ((ClusterExample) ex).getEventTypeSourceMapping();
        } else {
            throw new RuntimeException("Unknown ExampleCEP implementation");
        }

        ArrayList<Source> sources = ex.getSources();
        HashMap<String,Long> estimatedArrivalTime = ex.getEstimated();

        PatternTrie trieTree = new PatternTrie();
        trieManager.setPt(trieTree);

        ArrayList<String> queriesToRun = new ArrayList<>();
//        queriesToRun.add("src/main/resources/abc.query");
//        queriesToRun.add("src/main/resources/ab+c.query");
//        queriesToRun.add("src/main/resources/a+b+c.query");
//        queriesToRun.add("src/main/resources/bca.query");
//        queriesToRun.add("src/main/resources/c+a+b.query");
        if(pattern.equalsIgnoreCase("abc"))
            queriesToRun.add("src/main/resources/abc.query");
        if(pattern.equalsIgnoreCase("ab+c"))
            queriesToRun.add("src/main/resources/ab+c.query");
        if(pattern.equalsIgnoreCase("a+b+c"))
            queriesToRun.add("src/main/resources/a+b+c.query");
        if(pattern.equalsIgnoreCase("ad"))
            queriesToRun.add("src/main/resources/ad.query");
        if(pattern.equalsIgnoreCase("af"))
            queriesToRun.add("src/main/resources/af.query");
        if(pattern.equalsIgnoreCase("ag"))
            queriesToRun.add("src/main/resources/ag.query");
        if(pattern.equalsIgnoreCase("ae"))
            queriesToRun.add("src/main/resources/ae.query");
        if(pattern.equalsIgnoreCase("clusterq1"))
            queriesToRun.add("src/main/resources/cluster-q1.query");
        if(pattern.equalsIgnoreCase("clusterq2"))
            queriesToRun.add("src/main/resources/cluster-q2.query");


        String engine_Exp = engine.equalsIgnoreCase("LIMECEP")?suggestPatterns?"lc-oe":"lc":engine.toLowerCase();
        generalStats = new StatisticManager(0.6,0.2,0.2,2.5,engine_Exp,pattern,windowSize,dataset);
        generalStats.initializeManager(ex.getListofTypes());
        generalStats.setEstimated(estimatedArrivalTime);

        initialization(queriesToRun,sources,engine,isOOO,pattern,policy,oooType,isSimple, withCorrection, trieTree);

        consumers = new HashMap<>();
        threadsConsumers = new HashMap<>();

        initializeConsumers(sources,engine);

        startConsumerThreads();

    }
    public static void initialization(ArrayList<String> queriesToRun, ArrayList<Source> sources, String engine, Boolean isOOO, String query, String policy, String oooType, Boolean isSimple, Boolean withCorrection, PatternTrie patternTrie) {
        System.out.println("Starting engine: "+engine);
        if (engine.equalsIgnoreCase("LIMECEP")) {
            initializeLIMECEP(queriesToRun, sources, policy, withCorrection, patternTrie);
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

    public static void initializeLIMECEP(ArrayList<String> queriesToRun, ArrayList<Source> sources, String policy, Boolean withCorrection, PatternTrie patternTrie){
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
            String simple_pattern = "";
            for (String tptrans : em.getQuery().getTransitions().keySet()) {
                System.out.println(em_id + ": tptrans = " + tptrans + " TRANSITION: " + em.getQuery().getTransitions().get(tptrans).toString());
                simple_pattern+= tptrans;
            }
            trieManager.initializeTrie(simple_pattern);
            System.out.println(simple_pattern);
//            patternTrie.printTrie();
            System.out.println(em_id+": first_state = "+em.getFirstState());
            System.out.println(em_id+": last_state = "+em.getLastState());
            System.out.println();

            trieManager.initialize(
                    Main.globalProbStats,
                    trieManager.getThetaFreq(),
                    trieManager.getThetaRare(),
                    6,      // max pattern length
                    100      // exploration budget
            );

//            patternTrie.insert("abcd");
//
////            patternTrie.printTrie();
//
//            patternTrie.insert("abd");
//
////            patternTrie.printTrie();
//
//            patternTrie.pruneByConfidenceAndSupport();
//            patternTrie.printTrie();
//            patternTrie.printPatternStats();

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
//        String dataset = "src/main/resources/dataset";
//        dataset = ooo?dataset+"-"+oootype:dataset+"-test";
//        dataset = simple?dataset+"-simple":dataset;
//        dataset+= "-sase.stream";
        String dataset = "src/main/resources/nu_10t_1m_sase.stream";
        String policyShort = policy.equalsIgnoreCase("Skip-till-next-match")?"stnm":"stam";
        String[] generatedArgs = {
                "-q", "src/main/resources/sase-"+query+"-"+policyShort+".query", // Query file
                "-i", dataset,     // Input stream file path
                "-t", "stock",                  // Event type
                "-e", engine,                   // Engine type (sase or core)
                "-w",
//                "-o", "src/main/resources/output-"+engine+"-"+query+"-"+policyShort+".txt"       // Output file for results
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
            STS_counts.put(source.name(),new HashMap<>());
            for(String type: (ArrayList<String>)source.getEventTypes()){
                TreeSet<ABCEvent> tree = new TreeSet<>(new TimestampComparator());
                STS.get(source.name()).put(type,tree);
                STS_counts.get(source.name()).put(type,new HashMap<>());
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

        ensureEventStructures(e);

        generalStats.updateStats(e);
        globalProbStats.update(e.getEventType());

        eventCounter++;

        if (suggestPatterns && eventCounter % BATCH_SIZE == 0) {
            // Proposed full LC-OE
//            trieManager.runOperatorBatch();

            // Ablation variants
//             trieManager.runBatchAblation(false, true,  true,  true, true,  true);  // No Eligible Types
//             trieManager.runBatchAblation(true,  false, true,  true, true,  true);  // No Importance Filter
//             trieManager.runBatchAblation(true,  true,  false, true, true,  true);  // No Bottlenecks
            // trieManager.runBatchAblation(true,  true,  true,  true, false, false); // Extension Only
            // trieManager.runBatchAblation(true,  true,  true,  true, true,  false); // Extension + Variation
            // trieManager.runBatchAblation(true,  true,  true,  true, false, true); // Extension + Swap
            // trieManager.runBatchAblation(true,  true,  true,  false, true, true); // Variation + Swap
            // trieManager.runBatchAblation(true, true, true, false, true, false);  // Variation Only
            // trieManager.runBatchAblation(true, true, true, false, false, true);  // Swap Only
            // trieManager.runBatchAblation(true,  true,  true,  true, true,  true); // ALL operators



            // Exhaustive / Apriori-style baseline
//             trieManager.runAprioriBatch();

            switch (operatorConfiguration.toLowerCase()) {

                case "ext":
                    trieManager.runBatchAblation(
                            true, true, true,
                            true, false, false
                    );
                    break;

                case "var":
                    trieManager.runBatchAblation(
                            true, true, true,
                            false, true, false
                    );
                    break;

                case "swap":
                    trieManager.runBatchAblation(
                            true, true, true,
                            false, false, true
                    );
                    break;

                case "ext_var":
                    trieManager.runBatchAblation(
                            true, true, true,
                            true, true, false
                    );
                    break;

                case "ext_swap":
                    trieManager.runBatchAblation(
                            true, true, true,
                            true, false, true
                    );
                    break;

                case "var_swap":
                    trieManager.runBatchAblation(
                            true, true, true,
                            false, true, true
                    );
                    break;

                case "all":
                    trieManager.runOperatorBatch();
                    break;

                case "exhaustive":
                    trieManager.runAprioriBatch();
                    break;

                default:
                    throw new IllegalArgumentException(
                            "Unknown operator configuration: "
                                    + operatorConfiguration
                    );
            }
        }

        // CEP execution for query events
        if(etq.containsKey(e.getType())) {

            for(String query: etq.get(e.getType())) {
                evManagers.get(query).acceptEvent(e.getSource(), e);
            }

        } else {
            evaluateUnknownEvent(e);
        }

        Set<String> activated = trieManager.getActivatedPatterns();

        for(String pattern : activated) {

            if(pattern.endsWith(e.getEventType())) {

                trieManager.evaluateEvent(
                        updateAlgorithm,
                        pattern,
                        e,
                        queries.values().iterator().next().getWithin(),
                        evManagers.values().iterator().next().getPolicy()
                );
            }
        }
    }

//    public static void runBatchExploration() {
//
//        System.out.println("=== RUNNING BATCH EXPLORATION ===");
//
//        for (String qid : qte.keySet()) {
//
//            String basePattern = String.join("", qte.get(qid));
//
//            for (ABCEvent e : explorationBuffer) {
//
//                if (!basePattern.contains(e.getType())) {
//
//                    trieManager.evaluateEvent(
//                            updateAlgorithm,
//                            basePattern,
//                            e,
//                            queries.get(qid).getWithin(),
//                            evManagers.get(qid).getPolicy()
//                    );
//                }
//            }
//        }
//
//        System.out.println("=== BATCH DONE ===");
//    }

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


    public static void evaluateUnknownEvent(ABCEvent e) {
//        System.out.println("EVALUATING UNKNOWN EVENT "+e+" OF TYPE "+e.getEventType());
        for(String qid: qte.keySet()){
            String query = "";
            for (String evt: qte.get(qid))
                query+=evt;
//            System.out.println("Extracted query: "+query);
            if(!query.contains(e.getType())) {
                Main.STS.get(e.getSource()).computeIfAbsent(e.getType(), k -> new TreeSet<>(new TimestampComparator()));
                Main.STS.get(e.getSource()).get(e.getEventType()).add(e);
//                trieManager.evaluateEvent(updateAlgorithm,query, e, queries.get(qid).getWithin(), evManagers.get(qid).getPolicy());
            }
        }
    }

    private static void ensureEventStructures(ABCEvent e) {

        String source = e.getSource();
        String type = e.getEventType();

        STS.computeIfAbsent(source, k -> new HashMap<>());
        STS_counts.computeIfAbsent(source, k -> new HashMap<>());

        STS.get(source)
                .computeIfAbsent(type,
                        k -> new TreeSet<>(new TimestampComparator()));

        STS_counts.get(source)
                .computeIfAbsent(type,
                        k -> new HashMap<>());
    }

    public static void updateTrie(ArrayList<ABCEvent> m) {
        trieManager.insertMatch(m);
    }
}