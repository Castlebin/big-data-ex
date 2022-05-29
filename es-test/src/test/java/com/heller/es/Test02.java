package com.heller.es;

import static com.heller.es.EsServerConfig.HOST;
import static com.heller.es.EsServerConfig.PORT;
import static com.heller.es.EsServerConfig.SCHEMA;

import java.io.IOException;
import java.util.Arrays;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.heller.es.bo.User;

/**
 * Es 文档 （doc）的基本操作，使用 RestHighLevelClient
 *
 * Es 文档相当于关系型数据库表中的一行行数据，每一条数据都是一个文档。
 * 文档依附于索引  （关系型数据库，每行数据依附于表）
 */
public class Test02 {

    private static RestHighLevelClient client;
    private static ObjectMapper objectMapper;

    @BeforeAll
    static void beforeAll() {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(HOST, PORT, SCHEMA)));
        objectMapper = new ObjectMapper();
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
     * 新增文档 (如果索引不存在，es会自动创建索引)
     * （新增，_result 返回 CREATED。如果指定id的文档已经存在，则会更新该文档，_result 返回 UPDATED）
     */
    @Test
    void createDoc() throws IOException {
        // 创建数据对象
        User user = new User();
        user.setName("张三");
        user.setAge(18);
        user.setSex("男");
        String productJson = objectMapper.writeValueAsString(user);

        IndexRequest request = new IndexRequest();
        // 设置索引和id。这里如果不设置id的话，表示使用ES自动生成的id来创建这个文档
        request.index("user").id("1001");
        // 添加文档数据，数据格式为 JSON 格式
        request.source(productJson, XContentType.JSON);

        // 客户端发送请求，获取响应对象
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);

        // 打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());
    }

    /**
     * 更新指定id文档 (Es 6 不支持这个 RestHighLevelClient 操作，可以用上面的 IndexRequest 操作代替)
     */
    @Test
    void updateDoc() throws IOException {
        // 修改文档，请求参数
        UpdateRequest request = new UpdateRequest();
        request.index("user").id("1001");
        // 设置请求体，修改文档指定的属性
        request.doc(XContentType.JSON, "name", "李四", "age", 20);

        // 客户端发送请求，获取响应对象
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        // 打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_id:" + response.getId());
        System.out.println("_result:" + response.getResult());
    }

    /**
     * 获取指定id文档 （幂等操作，文档不存在，也不会抛出异常）
     */
    @Test
    void getDoc() throws IOException {
        // 1. 创建请求对象（可以看到，其实可以链式操作，更简洁）
        GetRequest request = new GetRequest()
                .index("user")
                .id("1001");

        // 2. 客户端发送请求，获取响应对象
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3. 打印结果信息
        System.out.println("_index:" + response.getIndex());
        System.out.println("_type:" + response.getType());
        System.out.println("_id:" + response.getId());
        System.out.println("source:" + response.getSourceAsString());
    }

    /**
     * 删除指定id文档
     */
    @Test
    void deleteDoc() throws IOException {
        //创建请求对象
        DeleteRequest request = new DeleteRequest().index("user").id("1001");
        //客户端发送请求，获取响应对象
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        //打印信息
        System.out.println(response.toString());
    }

    /**
     * 批量新增
     */
    @Test
    void bulkCreate() throws IOException {
        // 创建请求对象
        BulkRequest request = new BulkRequest();
        // 添加批量请求
        request.add(new IndexRequest("user").id("1001").source(XContentType.JSON, "name", "张三", "age", 18));
        request.add(new IndexRequest("user").id("1002").source(XContentType.JSON, "name", "李四", "age", 20));
        request.add(new IndexRequest("user").id("1003").source(XContentType.JSON, "name", "王五", "age", 22));

        //客户端发送请求，获取响应对象
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        //打印结果信息
        System.out.println("took:" + responses.getTook());
        System.out.println("items:" + Arrays.toString(responses.getItems()));
    }

    /**
     * 批量删除
     */
    @Test
    void bulkDelete() throws IOException {
        // 创建请求对象
        BulkRequest request = new BulkRequest();
        // 添加批量请求
        request.add(new DeleteRequest("user").id("1002"));
        request.add(new DeleteRequest("user").id("1003"));

        //客户端发送请求，获取响应对象
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        //打印结果信息
        System.out.println("took:" + responses.getTook());
        System.out.println("items:" + Arrays.toString(responses.getItems()));
    }

    /**
     * 批量操作（混合），可以看到 BulkRequest 只是将批量请求封装成了一个对象，将多个请求打包发送而已
     */
    @Test
    void bulkMixed() throws IOException {
        // 创建请求对象
        BulkRequest request = new BulkRequest();
        // 添加批量请求
        request.add(new DeleteRequest("user").id("1002"));
        request.add(new DeleteRequest("user").id("1003"));

        request.add(new IndexRequest("user").id("1001").source(XContentType.JSON, "name", "张三", "age", 18));
        request.add(new IndexRequest("user").id("1002").source(XContentType.JSON, "name", "李四", "age", 20));
        request.add(new IndexRequest("user").id("1003").source(XContentType.JSON, "name", "王五", "age", 22));

        request.add(new UpdateRequest("user", "1001").doc(XContentType.JSON, "name", "李四", "age", 18));

        request.add(new IndexRequest("user").id("1004").source(XContentType.JSON, "name", "赵六", "age", 24));
        request.add(new UpdateRequest("user", "1005").doc(XContentType.JSON, "name", "七七", "age", 24));
        request.add(new UpdateRequest("user", "1005").doc(XContentType.JSON, "name", "八八", "age", 24));

        //客户端发送请求，获取响应对象
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        //打印结果信息
        System.out.println("took:" + responses.getTook());
        System.out.println("items:" + Arrays.toString(responses.getItems()));
    }

}