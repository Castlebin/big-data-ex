package com.heller.es;

import static com.heller.es.EsServerConfig.HOST;
import static com.heller.es.EsServerConfig.PORT;
import static com.heller.es.EsServerConfig.SCHEMA;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Test00 {

    public static void main(String[] args) throws IOException {
        // 创建客户端对象
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(HOST, PORT, SCHEMA)));
        //		...
        System.out.println(client);

        // 关闭客户端连接
        client.close();
    }

}