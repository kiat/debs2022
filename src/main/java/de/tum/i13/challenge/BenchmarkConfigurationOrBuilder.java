// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: main/proto/challenger.proto

package de.tum.i13.challenge;

public interface BenchmarkConfigurationOrBuilder extends
    // @@protoc_insertion_point(interface_extends:Challenger.BenchmarkConfiguration)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <pre>
   *Token from the webapp for authentication
   * </pre>
   *
   * <code>string token = 1;</code>
   * @return The token.
   */
  java.lang.String getToken();
  /**
   * <pre>
   *Token from the webapp for authentication
   * </pre>
   *
   * <code>string token = 1;</code>
   * @return The bytes for token.
   */
  com.google.protobuf.ByteString
      getTokenBytes();

  /**
   * <pre>
   *chosen by the team, listed in the results
   * </pre>
   *
   * <code>string benchmark_name = 2;</code>
   * @return The benchmarkName.
   */
  java.lang.String getBenchmarkName();
  /**
   * <pre>
   *chosen by the team, listed in the results
   * </pre>
   *
   * <code>string benchmark_name = 2;</code>
   * @return The bytes for benchmarkName.
   */
  com.google.protobuf.ByteString
      getBenchmarkNameBytes();

  /**
   * <pre>
   *benchmark type, e.g., test
   * </pre>
   *
   * <code>string benchmark_type = 3;</code>
   * @return The benchmarkType.
   */
  java.lang.String getBenchmarkType();
  /**
   * <pre>
   *benchmark type, e.g., test
   * </pre>
   *
   * <code>string benchmark_type = 3;</code>
   * @return The bytes for benchmarkType.
   */
  com.google.protobuf.ByteString
      getBenchmarkTypeBytes();

  /**
   * <code>repeated .Challenger.Query queries = 4;</code>
   * @return A list containing the queries.
   */
  java.util.List<de.tum.i13.challenge.Query> getQueriesList();
  /**
   * <code>repeated .Challenger.Query queries = 4;</code>
   * @return The count of queries.
   */
  int getQueriesCount();
  /**
   * <code>repeated .Challenger.Query queries = 4;</code>
   * @param index The index of the element to return.
   * @return The queries at the given index.
   */
  de.tum.i13.challenge.Query getQueries(int index);
  /**
   * <code>repeated .Challenger.Query queries = 4;</code>
   * @return A list containing the enum numeric values on the wire for queries.
   */
  java.util.List<java.lang.Integer>
  getQueriesValueList();
  /**
   * <code>repeated .Challenger.Query queries = 4;</code>
   * @param index The index of the value to return.
   * @return The enum numeric value on the wire of queries at the given index.
   */
  int getQueriesValue(int index);
}
