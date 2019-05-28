package com.github.nikos.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
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

        StreamsBuilder builder = new StreamsBuilder();

        StoreBuilder sampleStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("inmemory-samples"),
                        Serdes.String(),
                        Serdes.String()
                );
        builder.addStateStore(sampleStoreBuilder);

        StoreBuilder counterStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("inmemory-counter"),
                        Serdes.String(),
                        Serdes.Long()
                );
        builder.addStateStore(counterStoreBuilder);

        KStream<String, String> dataInput = builder.stream("topic-025-input");

        dataInput.transform(new ReservoirSamplingSupplier(50L)).to("topic-025-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.cleanUp(); //ONLY FOR DEV
        streams.start();

        // printing the topology
        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }



    private static final class ReservoirSamplingSupplier implements TransformerSupplier<String, String, KeyValue<String, String>> {

        private final long k;

        private ReservoirSamplingSupplier(long k) {
            this.k = k;
        }

        @Override
        public Transformer<String, String, KeyValue<String, String>> get() {
            return new Transformer<String, String, KeyValue<String, String>>() {

                private boolean initIsDone;
                private List<String> sampleList = new ArrayList<String>();
                private ProcessorContext context;
                private int counter;

                @Override
                @SuppressWarnings("unchecked")
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(Duration.ofSeconds(2), PunctuationType.STREAM_TIME, this::punctuate);
                }

                @Override
                public KeyValue<String, String> transform(final String recordKey, final String recordValue) {

//            1) Create an array reservoir[0..k-1] and copy first k items of stream[] to it.
//            2) Now one by one consider all items from (k+1)th item to nth item.
//            …a) Generate a random number from 0 to i where i is index of current item in stream[]. Let the generated random number is j.
//            …b) If j is in range 0 to k-1, replace reservoir[j] with arr[i]

                    // System.out.println("Here sampling of " + k + " samples should happen " + recordKey + ": " + recordValue);

                    if (firstKItemsOfStream()) {
                        saveSampleInStoreWithCurrentKeyValuePair(recordKey, recordValue);
                    }
                    else {
                        int i = getIndexOfCurrentItem();
                        int j = generateRandomNumberFromZeroToIndexOfCurrentItem(i);

                        if (j>0 && j<k-1) {
                            replaceSampleInStoreWithCurrentKeyValuePair(j, recordKey, recordValue);
                        }
                    }

                    saveTheCounterInStore();
                    return null;

                }

                private void saveTheCounterInStore() {
                    counter++;
                }

                private boolean firstKItemsOfStream() {
                    if (counter < k)
                        return true;
                    else
                        return false;
                }

                private void saveSampleInStoreWithCurrentKeyValuePair(String recordKey, String recordValue) {
                    sampleList.add(recordValue);
                }

                private int getIndexOfCurrentItem() {
                    int indexOfCurrentItem = counter;
                    return indexOfCurrentItem;
                }

                private int generateRandomNumberFromZeroToIndexOfCurrentItem(int i) {

                    Random random = new Random();
                    int n = random.nextInt(i);

                    return n;
                }

                private void replaceSampleInStoreWithCurrentKeyValuePair(int j, String recordKey, String recordValue) {
                    sampleList.set(j, recordValue);
                }


                private void punctuate(final long timestamp) {

                    if (initIsDone) {
                        System.out.println("Punctuating @ timestamp {} " + timestamp);
                        sendAndPurgeAnyWaitingRecordsThatHaveExceededWaitTime(timestamp);
                    }

                    initIsDone = true;
                }

                private void sendAndPurgeAnyWaitingRecordsThatHaveExceededWaitTime(final long currentStreamTime) {
                    int count=0;
                    for(String value : sampleList){
                         context.forward(null, value);
                         count++;
                    }
                    System.out.println("###########################################");
                    System.out.println(count +" messages sent!");
                    System.out.println("###########################################");
                    counter = 0;
                    sampleList = new ArrayList<>();

                }

//                private boolean waitTimeExpired(final Instant recordTimestamp, final long currentStreamTime) {
//                    return Duration.between(recordTimestamp, Instant.ofEpochMilli(currentStreamTime))
//                            .compareTo(approxMaxWaitTimePerRecordForTableData) > 0;
//                }

                @Override
                public void close() {
                    // Not needed.
                }
            };

        }
    }

}
