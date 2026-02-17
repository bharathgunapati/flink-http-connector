package io.flink.connector.http.client;

import io.flink.connector.http.model.HttpExchange;
import io.flink.connector.http.model.HttpResponse;
import io.flink.connector.http.model.HttpSinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HttpSinkRequestSubmitterTest {

    @Mock
    private HttpRequestExecutor requestExecutor;

    private HttpSinkRequestSubmitter submitter;

    @AfterEach
    void tearDown() {
        if (submitter != null) {
            submitter.close();
        }
    }

    @Test
    void submitRequests_returnsOneExchangePerRecord() {
        HttpSinkRecord record = HttpSinkRecord.builder()
                .method("POST")
                .url("https://example.com")
                .headers(Map.of())
                .body(Map.of("key", "value"))
                .build();

        HttpResponse response = HttpResponse.builder()
                .statusCode(200)
                .body("ok")
                .build();

        when(requestExecutor.execute(any())).thenReturn(CompletableFuture.completedFuture(response));

        submitter = new HttpSinkRequestSubmitter(requestExecutor);

        CompletableFuture<List<HttpExchange>> future = submitter.submitRequests(List.of(record));
        List<HttpExchange> exchanges = future.join();

        assertEquals(1, exchanges.size());
        assertEquals(record, exchanges.get(0).getHttpSinkRecord());
        assertEquals(response, exchanges.get(0).getResponse());
        verify(requestExecutor, times(1)).execute(record);
    }

    @Test
    void submitRequests_returnsMultipleExchangesForMultipleRecords() {
        HttpSinkRecord record1 = HttpSinkRecord.builder()
                .method("GET")
                .url("https://example.com/1")
                .headers(Map.of())
                .body(Map.of())
                .build();
        HttpSinkRecord record2 = HttpSinkRecord.builder()
                .method("GET")
                .url("https://example.com/2")
                .headers(Map.of())
                .body(Map.of())
                .build();

        HttpResponse response = HttpResponse.builder().statusCode(200).body("ok").build();
        when(requestExecutor.execute(any())).thenReturn(CompletableFuture.completedFuture(response));

        submitter = new HttpSinkRequestSubmitter(requestExecutor);

        List<HttpExchange> exchanges = submitter.submitRequests(List.of(record1, record2)).join();

        assertEquals(2, exchanges.size());
        verify(requestExecutor, times(2)).execute(any());
    }

    @Test
    void close_invokesExecutorClose() {
        submitter = new HttpSinkRequestSubmitter(requestExecutor);
        submitter.close();

        verify(requestExecutor, times(1)).close();
    }
}
