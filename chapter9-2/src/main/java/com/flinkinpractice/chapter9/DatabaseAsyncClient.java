package com.flinkinpractice.chapter9;


import com.github.jasync.sql.db.QueryResult;
import com.github.jasync.sql.db.general.ArrayRowData;
import com.github.jasync.sql.db.mysql.MySQLConnection;
import com.github.jasync.sql.db.mysql.MySQLConnectionBuilder;
import com.github.jasync.sql.db.pool.ConnectionPool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class DatabaseAsyncClient extends RichAsyncFunction<String, Tuple2<String, String>> {
    private ConnectionPool<MySQLConnection> connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.disconnect().get();
        }
    }

    @Override
    public void asyncInvoke(String key, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        // issue the asynchronous request, receive a future for result
        String sql = "SELECT name, password FROM student WHERE name = '" + key + "';";
        CompletableFuture<QueryResult> result = connection.sendPreparedStatement(sql);

        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return Arrays.toString(((ArrayRowData) (result.get().getRows().get(0))).getColumns());
                } catch (InterruptedException | ExecutionException e) {
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult)));
        });
    }

    private static ConnectionPool<MySQLConnection> getConnection() {
        ConnectionPool<MySQLConnection> con = null;
        try {
            con = MySQLConnectionBuilder.createConnectionPool("jdbc:mysql://localhost:3306/flinkinpractice?useUnicode=true&characterEncoding=UTF-8&user=root&password=example");
        } catch (Exception e) {
            System.out.println("-----------mysql get conne ction has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
