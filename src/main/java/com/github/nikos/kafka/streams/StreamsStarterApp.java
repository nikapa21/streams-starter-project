package com.github.nikos.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class StreamsStarterApp {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        /*** We disable the cache to demonstrate all the "steps" involved in the transformation ***/
        /*** ONLY FOR DEVELOPMENT, NOT PRODUCTION ***/

        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        KStreamBuilder builder = new KStreamBuilder();
        Hashtable<String, Message> messageList = new Hashtable<>();

        Hashtable<Double[], String> coordsTable = new Hashtable<>();

        KStream<String, String> dataInput = builder.stream("topic-025-input");

        dataInput
                .selectKey((key, value) -> value.split(",")[1])
                .mapValues(line -> {
                    String[] fields = line.split(",");
                    Topic topic = new Topic("036");
                    String timestamp = fields[5];
                    String x = fields[6];
                    String y = fields[7];
                    Value value = new Value(x,y,timestamp);
                    Message msg = new Message(topic, value);
                    return msg; }).foreach((key, line) -> messageList.put(key, line));

//        dataInput.to("intermediary-topic-with-values-only");
//
//        KTable<String, String> dataValuesTable = builder.table("intermediary-topic-with-values-only");
//
//        KTable<String, String> dataValues = dataValuesTable;
//        KTable<String, Long> wordCounts = dataInput
//                .mapValues(value -> value
//                .toLowerCase())
//                .flatMapValues(value -> Arrays.asList(value.split(" ")))
//                .selectKey((ignoredKey, word) -> word)
//                .groupByKey()
//                .count("Counts");

        dataInput.to(Serdes.String(),Serdes.String(),"topic-025-output");
        KafkaStreams streams = new KafkaStreams(builder, config);

        streams.cleanUp(); //ONLY FOR DEV
        streams.start();

        // printing the topology
        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
