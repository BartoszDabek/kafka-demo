package pl.bdabek;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;

public class SystemInfoConsumer {
    public static void main(final String[] args) {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "system-info-application");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> cpuInfo = builder.stream("cpu");
        KStream<String, String> ramInfo = builder.stream("ram");

        monitorDataForEachNode(cpuInfo, ramInfo);

        KTable<Integer, Double> clusterCpuSum = cpuInfo
                .mapValues(Double::parseDouble)
                .selectKey((s, aDouble) -> 1)
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.Double()))
                .reduce(Double::sum, Materialized.with(Serdes.Integer(), Serdes.Double()));

        KTable<Integer, Integer> clusterCpuCount = cpuInfo
                .selectKey((s, aDouble) -> 1)
                .groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
                .aggregate(() -> 1, (integer, s, systemInfoConsumer) -> ++systemInfoConsumer, Materialized.with(Serdes.Integer(), Serdes.Integer()));

        KTable<Integer, Double> join = clusterCpuCount
                .join(clusterCpuSum, (integer, aDouble) -> aDouble / integer);

        join.toStream()
                .peek((s, aDouble) -> System.out.println("Srednie zuzycie cpu w klastze " + s + " = " + aDouble));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void monitorDataForEachNode(KStream<String, String> cpuInfo, KStream<String, String> ramInfo) {
        KTable<String, Double> sum = getSumForEachNode(cpuInfo);
        KTable<String, Integer> count = getCountForEachNode(cpuInfo);
        KTable<String, Double> cpuJoin = count
                .join(sum, (integer, aDouble) -> aDouble / integer);


        KTable<String, Double> ramSum = getSumForEachNode(ramInfo);
        KTable<String, Integer> ramCount = getCountForEachNode(ramInfo);
        KTable<String, Double> ramJoin = ramCount
                .join(ramSum, (integer, aDouble) -> aDouble / integer);

        cpuJoin.toStream()
                .peek((s, aDouble) -> System.out.println("Srednie zuzycie cpu na wezle " + s + " = " + aDouble));
        ramJoin.toStream()
                .peek((s, aDouble) -> System.out.println("Srednie zuzycie ram na wezle " + s + " = " + aDouble));
    }

    private static KTable<String, Integer> getCountForEachNode(KStream<String, String> cpuInfo) {
        return cpuInfo
                .groupByKey()
                .aggregate(
                        () -> 0,
                        (o, o2, integer) -> ++integer,
                        Materialized.with(Serdes.String(), Serdes.Integer()));
    }

    private static KTable<String, Double> getSumForEachNode(KStream<String, String> cpuInfo) {
        return cpuInfo
                .mapValues(Double::parseDouble)
                .groupByKey()
                .reduce(Double::sum, Materialized.with(Serdes.String(), Serdes.Double()));
    }
}
