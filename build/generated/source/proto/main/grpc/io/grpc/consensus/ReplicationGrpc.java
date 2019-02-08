package io.grpc.consensus;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.18.0)",
    comments = "Source: consensus.proto")
public final class ReplicationGrpc {

  private ReplicationGrpc() {}

  public static final String SERVICE_NAME = "consensus.Replication";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.grpc.consensus.RequestToPrepare,
      io.grpc.consensus.ReplyOfPrepared> getLeaderElectionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "leaderElection",
      requestType = io.grpc.consensus.RequestToPrepare.class,
      responseType = io.grpc.consensus.ReplyOfPrepared.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.grpc.consensus.RequestToPrepare,
      io.grpc.consensus.ReplyOfPrepared> getLeaderElectionMethod() {
    io.grpc.MethodDescriptor<io.grpc.consensus.RequestToPrepare, io.grpc.consensus.ReplyOfPrepared> getLeaderElectionMethod;
    if ((getLeaderElectionMethod = ReplicationGrpc.getLeaderElectionMethod) == null) {
      synchronized (ReplicationGrpc.class) {
        if ((getLeaderElectionMethod = ReplicationGrpc.getLeaderElectionMethod) == null) {
          ReplicationGrpc.getLeaderElectionMethod = getLeaderElectionMethod = 
              io.grpc.MethodDescriptor.<io.grpc.consensus.RequestToPrepare, io.grpc.consensus.ReplyOfPrepared>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "consensus.Replication", "leaderElection"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.consensus.RequestToPrepare.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.consensus.ReplyOfPrepared.getDefaultInstance()))
                  .setSchemaDescriptor(new ReplicationMethodDescriptorSupplier("leaderElection"))
                  .build();
          }
        }
     }
     return getLeaderElectionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.grpc.consensus.ValueProposed,
      io.grpc.consensus.ValueAccepted> getValueProposalMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "valueProposal",
      requestType = io.grpc.consensus.ValueProposed.class,
      responseType = io.grpc.consensus.ValueAccepted.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.grpc.consensus.ValueProposed,
      io.grpc.consensus.ValueAccepted> getValueProposalMethod() {
    io.grpc.MethodDescriptor<io.grpc.consensus.ValueProposed, io.grpc.consensus.ValueAccepted> getValueProposalMethod;
    if ((getValueProposalMethod = ReplicationGrpc.getValueProposalMethod) == null) {
      synchronized (ReplicationGrpc.class) {
        if ((getValueProposalMethod = ReplicationGrpc.getValueProposalMethod) == null) {
          ReplicationGrpc.getValueProposalMethod = getValueProposalMethod = 
              io.grpc.MethodDescriptor.<io.grpc.consensus.ValueProposed, io.grpc.consensus.ValueAccepted>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(
                  "consensus.Replication", "valueProposal"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.consensus.ValueProposed.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.grpc.consensus.ValueAccepted.getDefaultInstance()))
                  .setSchemaDescriptor(new ReplicationMethodDescriptorSupplier("valueProposal"))
                  .build();
          }
        }
     }
     return getValueProposalMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ReplicationStub newStub(io.grpc.Channel channel) {
    return new ReplicationStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ReplicationBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ReplicationBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ReplicationFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ReplicationFutureStub(channel);
  }

  /**
   */
  public static abstract class ReplicationImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Sends a request to seek consensus
     * </pre>
     */
    public void leaderElection(io.grpc.consensus.RequestToPrepare request,
        io.grpc.stub.StreamObserver<io.grpc.consensus.ReplyOfPrepared> responseObserver) {
      asyncUnimplementedUnaryCall(getLeaderElectionMethod(), responseObserver);
    }

    /**
     */
    public void valueProposal(io.grpc.consensus.ValueProposed request,
        io.grpc.stub.StreamObserver<io.grpc.consensus.ValueAccepted> responseObserver) {
      asyncUnimplementedUnaryCall(getValueProposalMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getLeaderElectionMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                io.grpc.consensus.RequestToPrepare,
                io.grpc.consensus.ReplyOfPrepared>(
                  this, METHODID_LEADER_ELECTION)))
          .addMethod(
            getValueProposalMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                io.grpc.consensus.ValueProposed,
                io.grpc.consensus.ValueAccepted>(
                  this, METHODID_VALUE_PROPOSAL)))
          .build();
    }
  }

  /**
   */
  public static final class ReplicationStub extends io.grpc.stub.AbstractStub<ReplicationStub> {
    private ReplicationStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ReplicationStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicationStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ReplicationStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a request to seek consensus
     * </pre>
     */
    public void leaderElection(io.grpc.consensus.RequestToPrepare request,
        io.grpc.stub.StreamObserver<io.grpc.consensus.ReplyOfPrepared> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getLeaderElectionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void valueProposal(io.grpc.consensus.ValueProposed request,
        io.grpc.stub.StreamObserver<io.grpc.consensus.ValueAccepted> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getValueProposalMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class ReplicationBlockingStub extends io.grpc.stub.AbstractStub<ReplicationBlockingStub> {
    private ReplicationBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ReplicationBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicationBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ReplicationBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a request to seek consensus
     * </pre>
     */
    public io.grpc.consensus.ReplyOfPrepared leaderElection(io.grpc.consensus.RequestToPrepare request) {
      return blockingUnaryCall(
          getChannel(), getLeaderElectionMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.grpc.consensus.ValueAccepted valueProposal(io.grpc.consensus.ValueProposed request) {
      return blockingUnaryCall(
          getChannel(), getValueProposalMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class ReplicationFutureStub extends io.grpc.stub.AbstractStub<ReplicationFutureStub> {
    private ReplicationFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ReplicationFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ReplicationFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ReplicationFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a request to seek consensus
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<io.grpc.consensus.ReplyOfPrepared> leaderElection(
        io.grpc.consensus.RequestToPrepare request) {
      return futureUnaryCall(
          getChannel().newCall(getLeaderElectionMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.grpc.consensus.ValueAccepted> valueProposal(
        io.grpc.consensus.ValueProposed request) {
      return futureUnaryCall(
          getChannel().newCall(getValueProposalMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_LEADER_ELECTION = 0;
  private static final int METHODID_VALUE_PROPOSAL = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ReplicationImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ReplicationImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_LEADER_ELECTION:
          serviceImpl.leaderElection((io.grpc.consensus.RequestToPrepare) request,
              (io.grpc.stub.StreamObserver<io.grpc.consensus.ReplyOfPrepared>) responseObserver);
          break;
        case METHODID_VALUE_PROPOSAL:
          serviceImpl.valueProposal((io.grpc.consensus.ValueProposed) request,
              (io.grpc.stub.StreamObserver<io.grpc.consensus.ValueAccepted>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class ReplicationBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ReplicationBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.grpc.consensus.ConsensusProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Replication");
    }
  }

  private static final class ReplicationFileDescriptorSupplier
      extends ReplicationBaseDescriptorSupplier {
    ReplicationFileDescriptorSupplier() {}
  }

  private static final class ReplicationMethodDescriptorSupplier
      extends ReplicationBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    ReplicationMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ReplicationGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ReplicationFileDescriptorSupplier())
              .addMethod(getLeaderElectionMethod())
              .addMethod(getValueProposalMethod())
              .build();
        }
      }
    }
    return result;
  }
}
