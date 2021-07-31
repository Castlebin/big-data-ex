package com.heller.es;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestSearchBasic {
    public static final String INDEX_NAME = "user";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        // 0. 创建一个 ES 客户端
        RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200)));

        // 1. 创建索引
        createEsIndex(esClient);

        // 2. 查询索引信息
        printEsIndexInfo(esClient);

        // 3. 添加数据
        addData(esClient);

        // 关闭 ES client
        esClient.close();
    }

    private static void createEsIndex(RestHighLevelClient esClient) throws IOException {
        boolean userIndexExist = esClient.indices()
                .exists(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
        if (userIndexExist) {
            System.out.println(INDEX_NAME + " 索引存在，不需要创建");
            //  esClient.indices().delete(new DeleteIndexRequest(SHOP_INDEX_NAME), RequestOptions.DEFAULT);
            return;
        }

        CreateIndexResponse createIndexResponse = esClient.indices()
                .create(new CreateIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
        System.out.println(INDEX_NAME + "创建索引操作是否成功：" + createIndexResponse.isAcknowledged());
    }

    private static void printEsIndexInfo(RestHighLevelClient esClient) throws IOException {
        GetIndexResponse getIndexResponse = esClient.indices()
                .get(new GetIndexRequest(INDEX_NAME), RequestOptions.DEFAULT);
        System.out.println(getIndexResponse.getMappings());
        System.out.println(getIndexResponse.getSettings());
    }

    private static void addData(RestHighLevelClient esClient) throws IOException {
        IndexRequest request = new IndexRequest();
        request.index("user").id("1001");

        User user = new User();
        user.setName("小明");
        user.setAge(18);
        user.setSex("男");

        request.source(OBJECT_MAPPER.writeValueAsString(user), XContentType.JSON);

        IndexResponse index = esClient.index(request, RequestOptions.DEFAULT);
        System.out.println(index.getResult());
    }

}
