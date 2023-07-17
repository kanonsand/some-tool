package org.example.kafkatool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.example.PrintUtil;
import sun.misc.Signal;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.example.PrintUtil.*;

public class KafkaManageTool extends ClosableHolder{

    private final ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(3,
            3, 60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1),
            new BasicThreadFactory.Builder().namingPattern("kafka-tool-task-%d").uncaughtExceptionHandler((t, e) -> printError(e)).build(),
            new ThreadPoolExecutor.AbortPolicy());

    private final AtomicBoolean NEED_EXIT = new AtomicBoolean(false);

    private final AtomicBoolean NEED_INTERRUPT = new AtomicBoolean();

    private final AtomicBoolean HAS_RUNNING_TASK = new AtomicBoolean();

    private Map<String, BiConsumer<String[], AdminClient>> ACTION_MAP = new HashMap<>();

    private static final String PROMPT = "$: ";

    private String host = null;

    private int port = 0;

    public static void main(String[] args) {
        if (args.length < 2) {
            printInfo("参数错误,java -jar XXX.jar host port");
            return;
        }
        KafkaManageTool kafkaManageTool = new KafkaManageTool();
        kafkaManageTool.start(args);
    }

    public void start(String[] args) {
        Signal.handle(new Signal("INT"),  // SIGINT
                signal -> {
                    if (HAS_RUNNING_TASK.get()) {
                        NEED_INTERRUPT.set(true);
                        clearLine();
                    }
                });
        init();
        host = args[0];
        port = Integer.parseInt(args[1]);
        try (AdminClient adminClient = getKafkaAdminClient(host, port)) {
            Scanner scanner = new Scanner(System.in);
            while (true) {
                newLine();
                String input = scanner.nextLine();
                if (StringUtils.isBlank(input)) {
                    continue;
                }
                input = input.trim();
                String[] split = input.split("\\s+");
                HAS_RUNNING_TASK.set(true);
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> handleInput(split, adminClient), EXECUTOR_SERVICE);
                while (true) {
                    try {
                        future.get();
                        break;
                    } catch (Exception e) {
                        printError(e);
                    }
                    if (NEED_INTERRUPT.get()) {
                        future.cancel(true);
                        break;
                    }
                }
                clearInterruptFlag();
                HAS_RUNNING_TASK.set(false);

                if (NEED_EXIT.get()) {
                    printInfo("Bye");
                    return;
                }
            }
        } catch (Exception e) {
            printError(e);
        } finally {
            EXECUTOR_SERVICE.shutdownNow();
        }

    }

    public static AdminClient getKafkaAdminClient(String host, int port) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
        return KafkaAdminClient.create(properties);
    }

    private void handleInput(String[] param,AdminClient adminClient) {
        String action = param[0];
        if (!ACTION_MAP.containsKey(action)) {
            printInfo(String.format("unknown command %s", action));
            return;
        }
        BiConsumer<String[], AdminClient> adminClientBiConsumer = ACTION_MAP.get(action);
        adminClientBiConsumer.accept(param, adminClient);
    }

    private void init() {
        ACTION_MAP.put("quit", (params, client) -> {
            NEED_EXIT.set(true);
        });
        ACTION_MAP.put("info", (params, client) -> {
            DescribeClusterOptions options = new DescribeClusterOptions();
            options.includeAuthorizedOperations(false);
            options.timeoutMs(10000);
            DescribeClusterResult describeClusterResult = client.describeCluster(options);
            try {
                Node node = describeClusterResult.controller().get();
                printInfo(node.toString());
            } catch (Exception e) {
                printError(e);
            }
            clearLine();

            try {
                Collection<Node> nodes = describeClusterResult.nodes().get();
                nodes.stream().map(Node::toString).forEach(PrintUtil::printInfo);
            } catch (Exception e) {
                printError(e);
            }
            clearLine();
        });
        ACTION_MAP.put("topic", (params, client) -> {
            try {
                if (params.length == 1) {
                    ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
                    listTopicsOptions.listInternal(false);
                    listTopicsOptions.timeoutMs(10000);
                    ListTopicsResult listTopicsResult = client.listTopics(listTopicsOptions);
                    List<String> topic = listTopicsResult.names().get().stream().sorted().collect(Collectors.toList());
                    printInfo(topic.toString());
                } else {
                    Set<String> nameSet = new HashSet<>(Arrays.asList(params).subList(1, params.length));
                    DescribeTopicsResult describeTopicsResult = client.describeTopics(nameSet);
                    Map<String, KafkaFuture<TopicDescription>> values = describeTopicsResult.values();
                    for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : values.entrySet()) {
                        try {
                            TopicDescription topicDescription = entry.getValue().get();
                            printInfo(topicDescription.toString());
                        } catch (Exception e) {
                            printError(e);
                        }
                        clearLine();
                    }
                }
            } catch (Exception e) {
                printError(e);
            }
        });
        ACTION_MAP.put("group", (params, client) -> {
            try {
                if (params.length == 1) {
                    ListConsumerGroupsOptions option = new ListConsumerGroupsOptions();
                    option.timeoutMs(10000);
                    ListConsumerGroupsResult result = client.listConsumerGroups(option);
                    KafkaFuture<Collection<ConsumerGroupListing>> all = result.all();
                    Collection<ConsumerGroupListing> consumerGroupListings = all.get();
                    for (ConsumerGroupListing consumerGroupListing : consumerGroupListings) {
                        printInfo(consumerGroupListing.toString());
                        clearLine();
                    }
                } else {
                    Set<String> nameSet = new HashSet<>(Arrays.asList(params).subList(1, params.length));
                    DescribeConsumerGroupsResult result = client.describeConsumerGroups(nameSet);
                    Map<String, KafkaFuture<ConsumerGroupDescription>> map = result.describedGroups();
                    for (Map.Entry<String, KafkaFuture<ConsumerGroupDescription>> entry : map.entrySet()) {
                        try {
                            ConsumerGroupDescription consumerGroupDescription = entry.getValue().get();
                            printInfo(consumerGroupDescription.toString());
                        } catch (Exception e) {
                            printError(e);
                        }
                        clearLine();
                    }
                }
            } catch (Exception e) {
                printError(e);
            }
        });
        ACTION_MAP.put("offset", (param, client) -> {
            try {
                if (param.length < 2) {
                    printInfo("please input consumer group");
                    return;
                }
                ListConsumerGroupOffsetsOptions option = new ListConsumerGroupOffsetsOptions();
                option.timeoutMs(10000);
                String consumerGroupName = param[1];
                ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(consumerGroupName, option);
                Map<TopicPartition, OffsetAndMetadata> metaData = result.partitionsToOffsetAndMetadata().get();
                KafkaConsumer<String, String> consumer = KafkaConsumerHolder.getConsumer(getHost(), getPort(), "test-tool", this);
                Map<TopicPartition, Long> topicPartitionLongMap = consumer.endOffsets(metaData.keySet());
                String format = "%-20s%-10s%-15s%-15s%-15s%-10s%s";
                printInfo(String.format(format, "TOPIC", "PARTITION", "CURRENT_OFFSET", "END_OFFSET", "LAG", "EPOCH", "METADATA"));
                metaData.forEach((k,v)->{
                    String topic = k.topic();
                    int partition = k.partition();
                    long end = Optional.ofNullable(topicPartitionLongMap.get(k)).orElse(-1L);
                    long offset = v.offset();
                    int leaderEpoch = v.leaderEpoch().orElse(-1);
                    String metadata = v.metadata();
                    printInfo(String.format(format, topic, partition, offset, end, end - offset, leaderEpoch, metadata));
                    clearLine();
                });
            } catch (Exception e) {
                printError(e);
            }
        });

        ACTION_MAP.put("help", (params, client) -> {
            printInfo("valid command:");
            String collect = ACTION_MAP.keySet().stream().sorted().collect(Collectors.joining(", "));
            printInfo(collect);
            clearLine();
        });
    }

    private void clearInterruptFlag() {
        NEED_INTERRUPT.set(false);
    }

    private static void newLine() {
        printPrefix(PROMPT);
    }

    private static void clearLine() {
        printInfo("");
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
