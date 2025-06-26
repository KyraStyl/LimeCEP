package kafka.consumer;


import events.ABCEvent;
import events.Source;
import events.TimestampComparator;
import handlers.KafkaMessageHandler;
import handlers.MessageHandlerRegistry;
import managers.EventManager;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumeInRangeMultipleTopics implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(VariableFetchSizeKafkaListener.class);
    private final List<String> topics;
    private final KafkaConsumer<String, String> consumer;
    private final long startTime;
    private final long endTime;
    private EventManager eventManager;
    private HashMap<String, TreeSet<ABCEvent>> treesets;

    public ConsumeInRangeMultipleTopics(List<String> topics, KafkaConsumer<String, String> consumer, EventManager eventManager, long startTime, long endTime) {
        this.topics = topics;
        this.consumer = consumer;
        this.startTime = startTime;
        this.endTime = endTime;
        this.eventManager = eventManager;
    }

    public ConsumeInRangeMultipleTopics(List<String> topics, String bootstrapServers, EventManager eventManager, long startTime, long endTime) {
        this(topics, defaultKafkaConsumer(bootstrapServers), eventManager, startTime, endTime);
    }

    private static KafkaConsumer<String, String> defaultKafkaConsumer(String boostrapServers) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    @Override
    public void run() {
        System.out.println("NEW CONSUMER - SUBSCRIBING TO TOPICS "+topics);
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Commit offsets here to ensure no message loss
                log.info("Committing offsets before partition revocation.");
                consumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("Partitions assigned: {}", partitions);
                Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
                partitions.forEach(partition -> timestampToSearch.put(partition, startTime));
                Map<TopicPartition, OffsetAndTimestamp> startOffsets = consumer.offsetsForTimes(timestampToSearch);
                startOffsets.forEach((partition, offsetTimestamp) -> {
                    if (offsetTimestamp != null) {
                        consumer.seek(partition, offsetTimestamp.offset());
                    } else {
                        // If no valid offset, start from the beginning
                        consumer.seekToBeginning(Arrays.asList(partition));
                    }
                });
            }
        });

        boolean continueConsuming = true;

        while (continueConsuming) {
            System.out.println("continue consuming");
            ConsumerRecords<String, String> records = null;
            try {
                records = consumer.poll(Duration.ofMillis(5000));
                System.out.println("i consumed "+records.count()+" events");
            } catch (WakeupException e) {
                // Ignore for shutdown
            }
            for (ConsumerRecord<String, String> record : records) {
                // Process each record here
                log.info("Consumed from Topic: {}, Partition: {}, Offset: {}, Timestamp: {}, Value: {}",
                        record.topic(), record.partition(), record.offset(), new Date(record.timestamp()), record.value());

                JSONObject message = new JSONObject(record.value());
                KafkaMessageHandler messageHandler = MessageHandlerRegistry.getHandler(record.topic());
                ArrayList<ABCEvent> eventsExtracted = messageHandler.processMessage(message);

                for(ABCEvent e: eventsExtracted){
                    String type = e.getType();
                    if(!treesets.containsKey(type))
                        treesets.put(type, new TreeSet<ABCEvent>(new TimestampComparator()));
                    treesets.get(type).add(e);
                    if(e.getTimestampDate().getTime() >= endTime)
                        continueConsuming = false;
                }

                System.out.println("end time == "+endTime+ " -- but event ts == "+record.timestamp());
                if (continueConsuming || record.timestamp() >= endTime) {
                    System.out.println("i have to stop consuming");
                    continueConsuming = false;
                    break;
                }

                // Manually commit the offset of the record just processed
                Map<TopicPartition, Long> currentOffset = Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        record.offset() + 1
                );
                consumer.commitSync();
            }
        }
        System.out.println("finished consuming");
        if (!treesets.isEmpty()) {
            this.eventManager.accept_onDemand(treesets);
            treesets.clear();
        }
        consumer.close();
    }

    public void setTreesets(HashMap<String, TreeSet<ABCEvent>> treesets) {
        this.treesets = (HashMap<String, TreeSet<ABCEvent>>) treesets.clone();
    }
}
