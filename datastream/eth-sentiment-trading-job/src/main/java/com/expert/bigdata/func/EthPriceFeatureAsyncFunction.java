package com.expert.bigdata.func;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.sql.*;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class EthPriceFeatureAsyncFunction extends RichAsyncFunction<String, String> {
    private final ObjectMapper mapper = new ObjectMapper();
    private String jdbcUrl;
    private String user;
    private String password;

    @Override
    public void open(Configuration parameters) {
        var params = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        jdbcUrl = params.get("mysql.url");
        user = params.get("mysql.user");
        password = params.get("mysql.password");
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) {
        CompletableFuture.runAsync(() -> {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
                ObjectNode node = (ObjectNode) mapper.readTree(input);
                long pubDate = node.get("pubDate").asLong();

                String sql = "SELECT rsi_14, atr_14, price_at_t FROM eth_kline_features WHERE timestamp <= ? ORDER BY timestamp DESC LIMIT 1";
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setLong(1, pubDate);
                ResultSet rs = ps.executeQuery();

                if (rs.next()) {
                    node.put("rsi_14", rs.getDouble("rsi_14"));
                    node.put("atr_14", rs.getDouble("atr_14"));
                    node.put("price_at_t", rs.getDouble("price_at_t"));
                    resultFuture.complete(Collections.singletonList(mapper.writeValueAsString(node)));
                } else {
                    resultFuture.complete(Collections.emptyList());
                }
            } catch (Exception e) {
                resultFuture.complete(Collections.emptyList());
            }
        });
    }
}
