package com.consumer.opensearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
 
    public static void main(String[] args) throws IOException {
        
        try(
            // OpenSearch client
            RestHighLevelClient openSearchClient = createOpenSearchClient();

            // Kafka Client
            KafkaConsumer<String, String> consumer = createKafkaConsumer();
        ) {
            // Create Index on OpenSearch
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if(!indexExists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("Wikimedia Index has been created");
            } else{
                logger.info("Wikimedia index already exists");
            }

            // we subscribe the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                logger.info("Received: " + recordCount + " records");

                for(ConsumerRecord<String, String> record: records){
                    // send the record into OpenSearch
                    try{
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                    .source(record.value(), XContentType.JSON);
                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        logger.info(response.getId());
                    }catch(Exception e){
                        
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error: ", e);
        }


        // close things
    }

    private static RestHighLevelClient createOpenSearchClient(){
        return new RestHighLevelClient(
            RestClient.builder(new HttpHost("http", "localhost", 9200)));
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(){
        String groupId = "consumer-opensearch-demo";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        return new KafkaConsumer<>(properties);
    }
}