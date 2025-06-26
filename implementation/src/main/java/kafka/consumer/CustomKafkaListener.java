package kafka.consumer;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import events.ABCEvent;
import events.Source;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import main.Main;

public class CustomKafkaListener<T> implements Runnable {
    private static final Logger log = Logger.getLogger(CustomKafkaListener.class.getName());
    private String topic;
    private KafkaConsumer<String, String> consumer;
    private Consumer<String> recordConsumer;
    private HashMap<String, Set<T>> messageSet;
    private ObjectMapper objectMapper;
    private Source<T> source;
    private String engine;


    public CustomKafkaListener(String topic, KafkaConsumer<String, String> consumer, HashMap<String, Set<T>> tree, Source<T> source, String engine) {
        this.topic = topic;
        this.consumer = consumer;
        this.recordConsumer = record -> log.info("received: " + record);
        this.messageSet = tree;
        this.objectMapper = new ObjectMapper();
        //this.eventManager = null;
        this.source = source;
        this.engine = engine;
    }

    public CustomKafkaListener(String topic, String bootstrapServers, HashMap<String, Set<T>> tree, Source source, String engine) {
        this(topic, defaultKafkaConsumer(bootstrapServers), tree, source, engine);
    }

    private static KafkaConsumer<String, String> defaultKafkaConsumer(String boostrapServers) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        return new KafkaConsumer<>(props);
    }

    public CustomKafkaListener onEach(Consumer<String> newConsumer) {
        recordConsumer = recordConsumer.andThen(newConsumer);
        return this;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(topic));
        try {
            while (!Thread.currentThread().isInterrupted()) {
                consumer.poll(Duration.ofMillis(100))
                    .forEach(this::processRecord);
            }
        } catch (WakeupException e) {
            // Ignore for shutdown
        } finally {
            consumer.close(); // Ensure the consumer is properly closed
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {

        String tt = "null";
        try {
                JSONObject message = new JSONObject(record.value());
                boolean terminate = message.has("Terminate");
                if (terminate) {
                    Main.printRMProfiling();
                    Main.printSMProfiling();
                    System.exit(100);
                }
                ArrayList<ABCEvent> eventsExtracted = source.processMessage(message);

                for (ABCEvent e : eventsExtracted) {
                    tt = e.getEventType();
                    if(messageSet.containsKey(tt.toLowerCase()))
                        messageSet.get(e.getEventType()).add((T) e);
                    else
                        continue;
                    if (this.engine.equals("LIMECEP"))
                        Main.updateManagers(e);
                    else if (this.engine.equals("SASE")) {
                        Main.runSASEonce(e);
                    } else if (this.engine.equals("SASEXT")) {
                        Main.runSASEXTonce(e);
                    }
                }
                consumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)));
        }catch (Exception ex){
            System.out.println("Failed to process record: "+ex.getMessage()+" for event e of type "+tt);
            ex.printStackTrace();
        }
    }

    public void shutdown() {
        consumer.wakeup(); // Used to break out of the poll loop
    }

}
