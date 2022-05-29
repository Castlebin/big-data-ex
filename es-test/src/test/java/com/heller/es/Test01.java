package com.heller.es;

import static com.heller.es.EsServerConfig.HOST;
import static com.heller.es.EsServerConfig.PORT;
import static com.heller.es.EsServerConfig.SCHEMA;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Es 索引的基本操作，使用 RestHighLevelClient
 *
 * Es 中的索引（Index），相当于关系型数据库中的 表 （Table）
 * ES 6 原来本来还有 Type 的概念，7 中已经删除，从 7 开始，索引就相当于 关系型数据库中的 表
 */
public class Test01 {

    private static RestHighLevelClient client;

    @BeforeAll
    static void beforeAll() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(HOST, PORT, SCHEMA)));
    }

    @AfterAll
    static void afterAll() {
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建索引
     */
    @Test
    void createIndex() throws IOException {
        // 创建索引 - 请求对象
        CreateIndexRequest request = new CreateIndexRequest("user2");
        // 发送请求，获取响应
        CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
        boolean acknowledged = response.isAcknowledged();
        // 响应状态
        System.out.println("操作状态 = " + acknowledged);
    }

    /**
     * 查询索引
     */
    @Test
    void queryIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("user2");
        GetIndexResponse response = client.indices().get(request, RequestOptions.DEFAULT);
        System.out.println("aliases: " + response.getAliases());
        System.out.println("mappings: " + response.getMappings());
        System.out.println("settings: " + response.getSettings());
    }

    /**
     * 删除索引
     */
    @Test
    void deleteIndex() throws IOException {
        // 删除索引 - 请求对象
        DeleteIndexRequest request = new DeleteIndexRequest("user2");
        // 发送请求，获取响应
        AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("操作状态 = " + response.isAcknowledged());
    }

}