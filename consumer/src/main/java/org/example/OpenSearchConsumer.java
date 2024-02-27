package org.example;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OpenSearchConsumer {

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "my-second-application";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        RestHighLevelClient elasticsearchClient = ElasticClient.getClient();
        try {
            boolean createIndex = ElasticClient.createIndex("wikimedia");
            if (createIndex) {
                log.info("created wikimedia index");
            } else {
                log.info("the index already exists");
            }
        } catch (Exception e) {
            log.error("something went wrong");
        }

        consumer.subscribe(Collections.singletonList("wikimedia.recentchange"));
        BulkRequest bulkRequest = null;
        while (true) {
            bulkRequest = new BulkRequest();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

            int recordCount = records.count();
            log.info("received " + recordCount + " records");

            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> document = new HashMap<>();
                document.put("key", record.key());
                document.put("value", record.value());
                String id = extractId(record.value());

                IndexRequest indexRequest = new IndexRequest("wikimedia")
                        .id(id)
                        .source(document);
                bulkRequest.add(indexRequest);
            }

            if (bulkRequest.numberOfActions() > 0) {
                try {
                    BulkResponse bulkResponse = elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    if (bulkResponse != null) {
                        log.info("Bulk response status: " + bulkResponse.status().getStatus());
                        log.info("inserted " + bulkResponse.getItems().length + " records");
                        if (bulkResponse.hasFailures()) {
                            log.error("Bulk response failures: " + bulkResponse.buildFailureMessage());
                        }
                    } else {
                        log.error("Received null response from Elasticsearch server");
                    }
                } catch (IOException e) {
                    log.error(e.getMessage());
                    e.printStackTrace();
                }
            } else {
                log.info("No actions to perform for bulk request");
            }

            consumer.commitSync();
            log.info("Offset is committed");
        }
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();
    }
}