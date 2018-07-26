package com.bazaarvoice.legion.hierarchy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.bazaarvoice.legion.hierarchy.model.HierarchySerdes;
import com.bazaarvoice.legion.hierarchy.model.Lineage;
import net.manub.embeddedkafka.EmbeddedKafka$;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Map$;
import scala.collection.immutable.HashMap$;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Runner {

    private final static Logger _log = LoggerFactory.getLogger(Runner.class);

    public static void main(String args[]) {

        setupLogging();

        Runner runner = new Runner();

        EmbeddedKafkaConfig embeddedKafkaConfig = EmbeddedKafkaConfig.apply(9092, 2181,
                runner.getCustomBrokerProperties(),
                runner.getCustomProducerProperties(),
                runner.getCustomConsumerProperties());

        final String bootstrapServers = "localhost:" + embeddedKafkaConfig.kafkaPort();

        EmbeddedKafka$.MODULE$.withRunningKafka(() -> {
            try (AdminClient adminClient = runner.createAdminClient(bootstrapServers)) {

                HierarchyStreamConfig hierarchyStreamConfig = new HierarchyStreamConfig(
                        "source", "child-parent-transition", "parent-child-transition", "children", "destination"
                );

                runner.createTopics(adminClient, hierarchyStreamConfig);
                KafkaStreams streams = new KafkaStreams(new TopologyGenerator().createTopology(hierarchyStreamConfig), runner.createConfig(bootstrapServers));
                streams.setUncaughtExceptionHandler((t, e) -> _log.error("Something failed", e));
                streams.start();
                _log.info("Streams started...");

                final ExecutorService service = Executors.newCachedThreadPool();
                runner.pollDestinationTopic(service, hierarchyStreamConfig.getDestTopic(), bootstrapServers);
                runner.startInteractiveHierarchyInput(service, hierarchyStreamConfig.getSourceTopic(),
                        hierarchyStreamConfig.getDestTopic(), bootstrapServers);

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    streams.close();
                    service.shutdownNow();
                    EmbeddedKafka$.MODULE$.stop();
                }));

                while (!service.isShutdown()) {
                    try {
                        service.awaitTermination(10, TimeUnit.SECONDS);
                    } catch (InterruptedException ignore) {
                        // ignore
                    }
                }
            }
            return null;
        }, embeddedKafkaConfig);
    }

    private static void setupLogging() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

        ConsoleAppender<ILoggingEvent> errAppender = new ConsoleAppender<>();
        errAppender.setTarget("System.err");
        errAppender.setContext(loggerContext);

        ConsoleAppender<ILoggingEvent> outAppender = new ConsoleAppender<>();
        outAppender.setTarget("System.out");
        outAppender.setContext(loggerContext);

        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(errAppender);
        rootLogger.setLevel(Level.WARN);
        rootLogger.setAdditive(true);

        ch.qos.logback.classic.Logger runnerLogger = loggerContext.getLogger("com.bazaarvoice");
        runnerLogger.addAppender(outAppender);
        runnerLogger.setLevel(Level.INFO);
        runnerLogger.setAdditive(true);

        loggerContext.start();
    }
    
    private scala.collection.immutable.Map<String, String> getCustomBrokerProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("broker.id", "0");
        props.put("num.network.threads", "3");
        props.put("num.io.threads", "8");
        props.put("num.partitions", "1");

        return HashMap$.MODULE$.apply(JavaConverters.asScalaBuffer(
                props.entrySet().stream().map(e -> new Tuple2<>(e.getKey(), e.getValue())).collect(Collectors.toList())));
    }

    private scala.collection.immutable.Map<String, String> getCustomProducerProperties() {
        return Map$.MODULE$.empty();
    }

    private scala.collection.immutable.Map<String, String> getCustomConsumerProperties() {
        return Map$.MODULE$.empty();
    }

    private StreamsConfig createConfig(String bootstrapServers) {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "category-hierarchy-prototype-" + System.currentTimeMillis());
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        settings.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        settings.put(StreamsConfig.POLL_MS_CONFIG, 100);
        settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        settings.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        return new StreamsConfig(settings);
    }

    private AdminClient createAdminClient(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(props);
    }

    private void createTopics(AdminClient adminClient, HierarchyStreamConfig hierarchyStreamConfig) {
        List<NewTopic> topics = new ArrayList<>();

        Map<String, String> topicCompaction = new LinkedHashMap<>();
        topicCompaction.put(hierarchyStreamConfig.getSourceTopic(), TopicConfig.CLEANUP_POLICY_COMPACT);
        topicCompaction.put(hierarchyStreamConfig.getParentTransitionTopic(), TopicConfig.CLEANUP_POLICY_COMPACT);
        topicCompaction.put(hierarchyStreamConfig.getChildTransitionTopic(), TopicConfig.CLEANUP_POLICY_DELETE);
        topicCompaction.put(hierarchyStreamConfig.getParentChildrenTopic(), TopicConfig.CLEANUP_POLICY_COMPACT);
        topicCompaction.put(hierarchyStreamConfig.getDestTopic(), TopicConfig.CLEANUP_POLICY_COMPACT);

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

    private void pollDestinationTopic(ExecutorService service, String destTopic, String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "FinalDestinationConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HierarchySerdes.LineageDeserializer.class.getName());
        Consumer<String, Lineage> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(destTopic));

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

    private void startInteractiveHierarchyInput(ExecutorService service, String sourceTopic, String destTopic, String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
                if (childParent.length == 1 && childParent[0].equalsIgnoreCase("dump")) {
                    // Convert to dump latest
                    childParent = new String[] { "dump", "latest" };
                }

                if (childParent[0].equalsIgnoreCase("dump") && childParent.length == 2) {
                    dumpDestination(childParent[1].equalsIgnoreCase("all"), destTopic, bootstrapServers);
                } else if (childParent.length % 2 != 0) {
                    System.out.println("Must provide a parent for every child");
                } else {
                    for (int i=0; i < childParent.length; i += 2) {
                        try {
                            producer.send(new ProducerRecord<>(sourceTopic, childParent[i], childParent[i+1])).get();
                            System.out.println("Parent of " + childParent[i] + " set to " + childParent[i+1]);
                        } catch (Exception e) {
                            _log.error("Failed to send record", e);
                        }
                    }
                }
                System.out.print("> ");
            }
            System.exit(0);
        });
    }

    private void dumpDestination(boolean full, String destTopic, String bootstrapServers) {
        String groupId = "DumpDest-" + UUID.randomUUID();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, HierarchySerdes.LineageDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, Lineage> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(destTopic));

        System.out.println("Dumping all destination records...");
        ConsumerRecords<String, Lineage> records;
        Map<String, List<String>> parents = new HashMap<>();
        while (!(records = consumer.poll(5000)).isEmpty()) {
            if (full) {
                records.forEach(record -> System.out.println(String.format("ID %s has parents %s", record.key(), record.value().getLinage())));
            } else {
                records.forEach(record -> parents.put(record.key(), record.value().getLinage()));
            }
        }
        parents.forEach((id, lineage) -> System.out.println(String.format("ID %s has parents %s", id, lineage)));
        System.out.println("End destination dump");

        consumer.unsubscribe();
    }
}
