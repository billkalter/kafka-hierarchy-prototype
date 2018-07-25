package com.bazaarvoice.legion.hierarchy;

import com.bazaarvoice.legion.hierarchy.model.ChildIdSet;
import com.bazaarvoice.legion.hierarchy.model.ChildTransition;
import com.bazaarvoice.legion.hierarchy.model.HierarchySerdes;
import com.bazaarvoice.legion.hierarchy.model.Lineage;
import com.bazaarvoice.legion.hierarchy.model.LineageTransition;
import com.bazaarvoice.legion.hierarchy.model.ParentTransition;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Runner {

    private final static Logger _log = LoggerFactory.getLogger(Runner.class);

    // Want to distinguish root categories from those where the parent is unknown (null)
    private final static String ROOT = "__root__";
    // Used in instances where the parent is undefined.
    private final static String UNDEF = "__undef__";

    public static void main(String args[]) {

        Runner runner = new Runner();

        try (AdminClient adminClient = runner.createAdminClient()) {

            runner.createTopics(adminClient);
            KafkaStreams streams = new KafkaStreams(runner.createToplogy(), runner.createConfig());
            streams.setUncaughtExceptionHandler((t, e) -> _log.error("Something failed", e));
            streams.start();
            _log.info("Streams started...");
            
            final ExecutorService service = Executors.newCachedThreadPool();
            runner.pollLineageTopic(service);
            runner.startInteractiveHierarchyInput(service);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    streams.close();
                    service.shutdownNow();
            }));
        }
    }

    private StreamsConfig createConfig() {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "category-hierarchy-prototype");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        settings.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        settings.put(StreamsConfig.POLL_MS_CONFIG, 10);
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/bill.kalter/work/kafka-hierarchy-prototype/target/state/kstream-state");
        return new StreamsConfig(settings);
    }

    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return AdminClient.create(props);
    }

    private void createTopics(AdminClient adminClient) {
        List<NewTopic> topics = new ArrayList<>();

        Map<String, String> topicCompaction = new LinkedHashMap<>();
        topicCompaction.put("child-parent", TopicConfig.CLEANUP_POLICY_COMPACT);
        topicCompaction.put("child-parent-transition", TopicConfig.CLEANUP_POLICY_COMPACT);
        topicCompaction.put("parent-child-transition", TopicConfig.CLEANUP_POLICY_DELETE);
        topicCompaction.put("parent-children", TopicConfig.CLEANUP_POLICY_COMPACT);
        topicCompaction.put("lineage", TopicConfig.CLEANUP_POLICY_COMPACT);

        for (Map.Entry<String, String> entry : topicCompaction.entrySet()) {
            String topicName = entry.getKey();
            String compaction = entry.getValue();

            Map<String, String> topicConfig = new HashMap<>();
            topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, compaction);

            NewTopic topic = new NewTopic(topicName, 1, (short) 1);
            topic.configs(topicConfig);

            topics.add(topic);
        }

        CreateTopicsResult results = adminClient.createTopics(topics);

        for (Map.Entry<String, KafkaFuture<Void>> entry : results.values().entrySet()) {
            try {
                entry.getValue().get();
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    _log.debug("Topic found to already exist: {}", entry.getKey());
                } else {
                    _log.error("Failed to create topic {}", entry.getKey(), e.getCause());
                }
            } catch (InterruptedException e) {
                _log.error("Failed to create topic {}", entry.getKey(), e);
                throw new RuntimeException(e);
            }
        }         
    }
    
    private Topology createToplogy() {

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> childParentTable = builder.table(
                "child-parent", Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, ParentTransition> childParentTransitionTable = builder.table(
                "child-parent-transition", Consumed.with(Serdes.String(), HierarchySerdes.ParentTransition()));

        KStream<String, ChildTransition> parentChildTransitionStream = builder.stream(
                "parent-child-transition", Consumed.with(Serdes.String(), HierarchySerdes.ChildTransition()));

        KTable<String, ChildIdSet> parentChildrenTable = builder.table(
                "parent-children", Consumed.with(Serdes.String(), HierarchySerdes.ChildIdSet()));

        KTable<String, Lineage> lineageTable = builder.table(
                "lineage", Consumed.with(Serdes.String(), HierarchySerdes.Lineage()));
        
        childParentTable
                .toStream()
                .leftJoin(
                        childParentTransitionTable,
                        (newParentId, priorTransition) -> new ParentTransition(
                                Optional.ofNullable(priorTransition).map(ParentTransition::getNewParentId).orElse(null),
                                parentOrRoot(newParentId)))
                .filter((child, parentTransition) -> !Objects.equals(parentTransition.getOldParentId(), parentTransition.getNewParentId()))
                .to("child-parent-transition", Produced.with(Serdes.String(), HierarchySerdes.ParentTransition()));

        childParentTransitionTable
                .toStream()
                .flatMap((KeyValueMapper<String, ParentTransition, Iterable<? extends KeyValue<String, ChildTransition>>>) (childId, parentTransition) -> {
                    List<KeyValue<String, ChildTransition>> transitions = new ArrayList<>(2);
                    if (parentTransition.getOldParentId() != null) {
                        transitions.add(new KeyValue<>(parentTransition.getOldParentId(), new ChildTransition(childId, false)));
                    }
                    transitions.add(new KeyValue<>(parentTransition.getNewParentId(), new ChildTransition(childId, true)));
                    return transitions;
                })
                .to("parent-child-transition", Produced.with(Serdes.String(), HierarchySerdes.ChildTransition()));
        
        // Root parents
        parentChildTransitionStream
                .filter((id, transition) -> isRoot(id) && transition.isAdd())
                .map((id, transition) -> new KeyValue<>(transition.getChildId(), Lineage.EMPTY))
                .to("lineage", Produced.with(Serdes.String(), HierarchySerdes.Lineage()));

        // Parents which don't yet exist
        parentChildTransitionStream
                .leftJoin(childParentTable, (childTransition, maybeParent) -> maybeParent != null)
                .filter((id, exists) -> !(exists || isRoot(id)))
                .map((id, ignore) -> new KeyValue<>(id, new Lineage(UNDEF)))
                .to("lineage", Produced.with(Serdes.String(), HierarchySerdes.Lineage()));

        parentChildTransitionStream
                .groupByKey(Serialized.with(Serdes.String(), HierarchySerdes.ChildTransition()))
                .aggregate(
                        () -> ChildIdSet.EMPTY,
                        (key, t, idSet) -> idSet.withUpdate(t.getChildId(), t.isAdd()),
                        Materialized.with(Serdes.String(), HierarchySerdes.ChildIdSet()))
                .toStream()
                .to("parent-children", Produced.with(Serdes.String(), HierarchySerdes.ChildIdSet()));

        parentChildrenTable
                .join(lineageTable, (children, lineage) -> new LineageTransition(lineage.getLinage(), children.children()))
                .toStream()
                .flatMap((KeyValueMapper<String, LineageTransition, Iterable<? extends KeyValue<String, Lineage>>>) (id, lineageTransition) -> {
                    List<String> updatedLinage = new ArrayList<>(lineageTransition.getParentLineage());
                    updatedLinage.add(id);
                    return lineageTransition.getChildren().stream()
                            .map(childId -> new KeyValue<>(childId, new Lineage(updatedLinage)))
                            .collect(Collectors.toList());
                })
                .to("lineage", Produced.with(Serdes.String(), HierarchySerdes.Lineage()));

        return builder.build();
    }

    private void pollLineageTopic(ExecutorService service) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "FinalChildLineageConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HierarchySerdes.LineageDeserializer.class.getName());
        Consumer<String, Lineage> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("lineage"));

        service.submit(() -> {
            while (!service.isShutdown()) {
                try {
                    ConsumerRecords<String, Lineage> records = consumer.poll(500);
                    records.forEach(record -> _log.info("ID {} has parents {}", record.key(), record.value().getLinage()));
                } catch (Exception e) {
                    _log.error("Failed to poll", e);
                }
            }
        });
    }

    private void startInteractiveHierarchyInput(ExecutorService service) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        service.submit(() -> {
            Scanner scanner = new Scanner(System.in);
            String line;
            System.out.println("> ");
            while (!service.isShutdown() && !(line = scanner.nextLine()).trim().equalsIgnoreCase("quit")) {
                String[] childParent = line.split("\\s");
                if (childParent.length != 2) {
                    System.out.println("Must provide child and parent");
                } else if (childParent[0].equals(childParent[1])) {
                    System.out.println("Child cannot be its own parent");
                } else if (childParent[0].equalsIgnoreCase("dump")) {
                    dumpLineage(childParent[1].equalsIgnoreCase("all"));
                } else {
                    try {
                        producer.send(new ProducerRecord<>("child-parent", childParent[0], childParent[1])).get();
                        System.out.println("Parent of " + childParent[0] + " set to " + childParent[1]);
                    } catch (Exception e) {
                        _log.error("Failed to send record", e);
                    }

                }
                System.out.print("> ");
            }
            System.exit(0);
        });
    }

    private void dumpLineage(boolean full) {
        String groupId = "DumpLineage-" + UUID.randomUUID();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HierarchySerdes.LineageDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, Lineage> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton("lineage"));

        System.out.println("Dumping all lineage records...");
        ConsumerRecords<String, Lineage> records;
        while (!(records = consumer.poll(1000)).isEmpty()) {
            if (full) {
                records.forEach(record -> System.out.println(String.format("ID %s has parents %s", record.key(), record.value().getLinage())));
            } else {
                Map<String, List<String>> parents = new HashMap<>();
                records.forEach(record -> parents.put(record.key(), record.value().getLinage()));
                parents.forEach((id, lineage) -> System.out.println(String.format("ID %s has parents %s", id, lineage)));
            }
        }
        System.out.println("End lineage dump");

        consumer.unsubscribe();
    }

    private static String parentOrRoot(String parentId) {
        return Optional.ofNullable(parentId).orElse(ROOT);
    }

    private static boolean isRoot(String id) {
        return ROOT.equals(id);
    }
}
