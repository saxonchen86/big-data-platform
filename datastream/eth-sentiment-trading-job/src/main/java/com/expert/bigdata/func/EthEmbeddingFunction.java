package com.expert.bigdata.func;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import java.util.Collections;

public class EthEmbeddingFunction extends RichAsyncFunction<String, String> {
    private final ObjectMapper mapper = new ObjectMapper();
    private String embeddingUrl;

    @Override
    public void open(Configuration parameters) {
        embeddingUrl = getRuntimeContext().getExecutionConfig().getGlobalJobParameters()
                .toMap().getOrDefault("ollama.embedding.url", "http://localhost:11434/api/embeddings");
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            ObjectNode node = (ObjectNode) mapper.readTree(input);
            String content = node.get("raw_content").asText();

            ObjectNode body = mapper.createObjectNode();
            body.put("model", "nomic-embed-text");
            body.put("prompt", content);

            HttpPost post = new HttpPost(embeddingUrl);
            post.setEntity(new StringEntity(mapper.writeValueAsString(body)));

            try (CloseableHttpResponse response = client.execute(post)) {
                JsonNode res = mapper.readTree(EntityUtils.toString(response.getEntity()));
                ArrayNode vector = (ArrayNode) res.get("embedding");
                node.set("vector", vector);
                resultFuture.complete(Collections.singletonList(mapper.writeValueAsString(node)));
            }
        } catch (Exception e) {
            resultFuture.complete(Collections.emptyList());
        }
    }
}