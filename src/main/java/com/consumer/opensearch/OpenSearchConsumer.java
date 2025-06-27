package com.consumer.opensearch;

import java.io.IOException;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
 
    public static void main(String[] args) throws IOException {
        // OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        
        try {
            // Create Index on OpenSearch
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if(!indexExists){
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("Wikimedia Index has been created");
            } else{
                logger.info("Wikimedia index already exists");
            }
        } catch (Exception e) {
            logger.error("Error: ", e);
        }
        
        // Kafka Client


        // close things
    }

   public static RestHighLevelClient createOpenSearchClient(){
        //Create a client.
        RestClientBuilder builder = RestClient.builder(new HttpHost("http", "localhost", 9200));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}