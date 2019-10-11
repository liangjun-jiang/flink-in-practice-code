package com.flinkinpractice.chapter11;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


@Slf4j
public class ElasticSearchSinkUtil {

    private static final String ES_SECURITY_USERNAME = "elastic";
    private static final String ES_SECURITY_PASSWORD = "changeme";

    public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func,
                                   ParameterTool parameterTool) {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

                            // elasticsearch username and password
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ES_SECURITY_USERNAME, ES_SECURITY_PASSWORD));

                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
                }
        );
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }

    /**
     * desc: find es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

    public static class RetryRejectedExecutionFailureHandler implements ActionRequestFailureHandler {
        private static final long serialVersionUID = -7423562912824511906L;

        public RetryRejectedExecutionFailureHandler() {
        }

        @Override
        public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                indexer.add(new ActionRequest[]{action});
            } else {
                if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
                    // omit the timeout of writing to ES. ElasticSearchSink will retry so it's not necessary to restart flink job to retry.
                    return;
                } else {
                    Optional<IOException> exp = ExceptionUtils.findThrowable(failure, IOException.class);
                    if (exp.isPresent()) {
                        IOException ioExp = exp.get();
                        if (ioExp != null && ioExp.getMessage() != null && ioExp.getMessage().contains("max retry timeout")) {
                            // request retries exceeded max retry timeout
                            // we tried different ES node and still no success. We give up. The data will be lost.
                            log.error(ioExp.getMessage());
                            return;
                        }
                    }
                }
                throw failure;
            }
        }
    }
}
