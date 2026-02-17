package io.flink.connector.http.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Represents an HTTP response received from a server or constructed for error cases.
 *
 * <p>Contains the HTTP status code and response body. Used as part of {@link HttpExchange} to
 * pair a request with its response.
 *
 * @see HttpExchange
 * @see HttpSinkRecord
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class HttpResponse implements Serializable {

  /** HTTP status code (e.g. 200, 404, 500), or null if no response was received (e.g. IO error). */
  private final Integer statusCode;

  /** Response body as string, or error message when constructed for a failed request. */
  private final String body;
}
