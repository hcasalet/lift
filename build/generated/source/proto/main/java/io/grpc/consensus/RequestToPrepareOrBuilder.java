// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: consensus.proto

package io.grpc.consensus;

public interface RequestToPrepareOrBuilder extends
    // @@protoc_insertion_point(interface_extends:consensus.RequestToPrepare)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string candidate = 1;</code>
   */
  java.lang.String getCandidate();
  /**
   * <code>string candidate = 1;</code>
   */
  com.google.protobuf.ByteString
      getCandidateBytes();

  /**
   * <code>int32 sequenceId = 3;</code>
   */
  int getSequenceId();
}
