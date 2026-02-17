package io.flink.connector.http.model;


import java.io.Serializable;

import io.flink.connector.http.client.HttpSinkRequestSubmitter;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Represents a complete HTTP exchange: the request ({@link HttpSinkRecord}) and its response
 * ({@link HttpResponse}).
 *
 * <p>Used by {@link HttpSinkRequestSubmitter} to return the
 * outcome of each HTTP request, including both the original request and the response (or error
 * details when the request fails).
 *
 * @see HttpSinkRecord
 * @see HttpResponse
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public final class HttpExchange implements Serializable {

  /** The HTTP request that was sent. */
  private final HttpSinkRecord httpSinkRecord;

  /** The HTTP response received, or an error response if the request failed. */
  private final HttpResponse response;
}
