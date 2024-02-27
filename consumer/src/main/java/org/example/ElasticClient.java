package org.example;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;

import java.io.IOException;

public class ElasticClient {
    public static RestHighLevelClient getClient() {
        RestHighLevelClient client = new RestHighLevelClientBuilder(
                RestClient.builder(new HttpHost("192.168.100.97", 9200, "http")).build())
                .setApiCompatibilityMode(true)
                .build();

        return client;
    }

    public static boolean createIndex(String indexTitle) {
        RestHighLevelClient client = getClient();
        try {
            CreateIndexRequest request = new CreateIndexRequest(indexTitle);
            CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
            return response.isAcknowledged();
        } catch (IOException e) {
            return false;
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                // Handle exception if necessary
            }
        }
    }
}