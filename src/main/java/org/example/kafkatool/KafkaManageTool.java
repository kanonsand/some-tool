package org.example.kafkatool;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.admin.*;
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

public class KafkaManageTool extends ClosableHolder {

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
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> executeCommand(split, adminClient), EXECUTOR_SERVICE);
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

    private void executeCommand(String[] param, AdminClient adminClient) {
        String action = param[0];
        if (!ACTION_MAP.containsKey(action)) {
            printInfo(String.format("unknown command %s", action));
            printInfo("valid command" + ACTION_MAP.keySet().stream().sorted().collect(Collectors.toList()));
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
            try {
                DescribeClusterOptions options = new DescribeClusterOptions();
                options.includeAuthorizedOperations(false);
                options.timeoutMs(10000);
                DescribeClusterResult describeClusterResult = client.describeCluster(options);
                Node node = describeClusterResult.controller().get();
                printInfo(node.toString());
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
                        Boolean listConsoleConsumer = KafkaToolConfig.getConfig("listConsoleConsumer");
                        if (consumerGroupListing.isSimpleConsumerGroup() && !Optional.ofNullable(listConsoleConsumer).orElse(false)) {
                            continue;
                        }
                        printInfo(consumerGroupListing.toString());
                    }
                    clearLine();
                } else {
                    Set<String> nameSet = new HashSet<>(Arrays.asList(params).subList(1, params.length));
                    DescribeConsumerGroupsResult result = client.describeConsumerGroups(nameSet);
                    Map<String, KafkaFuture<ConsumerGroupDescription>> map = result.describedGroups();
                    Map<String, ConsumerGroupDescription> descrbeMap = result.all().get(1L, TimeUnit.MINUTES);
                    descrbeMap.forEach((k, v) -> printInfo(k + " -> " + v));
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
                //获取当前offset
                ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(consumerGroupName, option);
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = result.partitionsToOffsetAndMetadata().get();
                //获取earliest
                Map<TopicPartition, OffsetSpec> earliestOffsetSpec = offsetAndMetadataMap.keySet().stream().collect(Collectors.toMap(e -> e, e -> OffsetSpec.earliest()));
                //获取latest
                Map<TopicPartition, OffsetSpec> latestOffsetSpec = offsetAndMetadataMap.keySet().stream().collect(Collectors.toMap(e -> e, e -> OffsetSpec.latest()));

                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffset = client.listOffsets(earliestOffsetSpec).all().get();
                Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffset = client.listOffsets(latestOffsetSpec).all().get();
                //整合结果
                Map<TopicPartition, Triple<Long, Long, Long>> offsetMap = new HashMap<>();
                offsetAndMetadataMap.forEach((k, v) -> {
                    ListOffsetsResult.ListOffsetsResultInfo earliestInfo = earliestOffset.get(k);
                    ListOffsetsResult.ListOffsetsResultInfo latestInfo = latestOffset.get(k);
                    offsetMap.put(k, new ImmutableTriple<>(earliestInfo.offset(), v.offset(), latestInfo.offset()));
                });

                String offsetFormat = "%-20s%-10s%-20s%-20s%-20s%-15s%-15s";
                printInfo(String.format(offsetFormat, "TOPIC", "PARTITION", "EARLIEST_OFFSET", "CURRENT_OFFSET", "END_OFFSET", "TO_HEAD", "LAG"));
                offsetMap.forEach((k, v) ->
                        printInfo(String.format(offsetFormat, k.topic(), k.partition(), v.getLeft(),
                                v.getMiddle(), v.getRight(), v.getMiddle() - v.getLeft(), v.getRight() - v.getMiddle())));
                clearLine();
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

        ACTION_MAP.put("set", (param, client) -> {
            if (param.length == 1) {
                KafkaToolConfig.printAll();
            } else {
                if (param.length < 3) {
                    printInfo("请输入选项名称和要修改的选项值");
                    return;
                }
                KafkaToolConfig.setConfig(param[1], param[2]);
            }
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
