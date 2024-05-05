package com.amazonaws.services.dynamodbv2.streamsadapter;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.AmazonServiceExceptionTransformer;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.DescribeStreamResultAdapter;

import software.amazon.awssdk.core.RequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.handler.ClientExecutionParams;
import software.amazon.awssdk.core.http.HttpResponseHandler;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollector;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.NoOpMetricCollector;
import software.amazon.awssdk.protocols.json.JsonOperationMetadata;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.paginators.ListStreamConsumersPublisher;
import software.amazon.awssdk.services.kinesis.paginators.ListStreamsPublisher;
import software.amazon.awssdk.services.kinesis.transform.DescribeStreamRequestMarshaller;
import software.amazon.awssdk.services.kinesis.waiters.KinesisAsyncWaiter;
import software.amazon.awssdk.utils.CompletableFutureUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class KinesisClientAdapter implements KinesisAsyncClient {
    private static final int DYNAMODB_STREAMS_RETENTION_PERIOD_HOURS = 24;
    DynamoDbStreamsClient dynamoStreamsClient;

    public KinesisClientAdapter(DynamoDbStreamsClient dynamoStreamsClient) {
        this.dynamoStreamsClient = dynamoStreamsClient;
    }

    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(DescribeStreamRequest kinesisRequest) {
        /*
            TODO: There was a request cache
            TODO: Handle disabled stream
            FIXME: Needs to return a Kinesis Response. REMEMBER?
         */
        
        try {
            software.amazon.awssdk.services.dynamodb.model.StreamDescription dynamoStreamDescription = dynamoStreamsClient.describeStream( request ->
                request
                        .streamArn(kinesisRequest.streamARN())
                        .exclusiveStartShardId(kinesisRequest.exclusiveStartShardId())
                        .limit(kinesisRequest.limit())
            ).streamDescription();


            return software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse.builder()
                    .streamDescription(StreamDescription.builder()
                            .enhancedMonitoring(new EnhancedMetrics[0])
                            .hasMoreShards(dynamoStreamDescription.lastEvaluatedShardId() == null || dynamoStreamDescription.lastEvaluatedShardId().isEmpty())
                            .retentionPeriodHours(DYNAMODB_STREAMS_RETENTION_PERIOD_HOURS)
                            .shards(
                                    dynamoStreamDescription.shards().stream().map(dynamoShard -> Shard.builder()
                                            .parentShardId(dynamoShard.parentShardId())
                                            .sequenceNumberRange(dynamoShard.sequenceNumberRange())
                                            .shardId(dynamoShard.shardId())
                                            .build()
                                ).collect(Collectors.toList())
                            )
                            .streamARN(dynamoStreamDescription.streamArn())
                            .streamCreationTimestamp(dynamoStreamDescription.creationRequestDateTime())
                            .streamName(dynamoStreamDescription.tableName())
                            .streamStatus("ENABLED")
                            .build()); // TODO: Map out the other statii

        } catch (AwsServiceException awsServiceException) {
            throw AmazonServiceExceptionTransformer.transformDynamoDBStreamsToKinesisDescribeStream(e);
        }
    }

    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(GetRecordsRequest getRecordsRequest) {
        SdkClientConfiguration clientConfiguration = this.updateSdkClientConfiguration(describeStreamRequest, this.clientConfiguration);
        List<MetricPublisher> metricPublishers = resolveMetricPublishers(clientConfiguration, (RequestOverrideConfiguration)describeStreamRequest.overrideConfiguration().orElse((Object)null));
        MetricCollector apiCallMetricCollector = metricPublishers.isEmpty() ? NoOpMetricCollector.create() : MetricCollector.create("ApiCall");

        try {
            ((MetricCollector)apiCallMetricCollector).reportMetric(CoreMetric.SERVICE_ID, "Kinesis");
            ((MetricCollector)apiCallMetricCollector).reportMetric(CoreMetric.OPERATION_NAME, "DescribeStream");
            JsonOperationMetadata operationMetadata = JsonOperationMetadata.builder().hasStreamingSuccessResponse(false).isPayloadJson(true).build();
            HttpResponseHandler<DescribeStreamResponse> responseHandler = this.protocolFactory.createResponseHandler(operationMetadata, DescribeStreamResponse::builder);
            HttpResponseHandler<AwsServiceException> errorResponseHandler = this.createErrorResponseHandler(this.protocolFactory, operationMetadata);
            CompletableFuture<DescribeStreamResponse> executeFuture = this.clientHandler.execute((new ClientExecutionParams()).withOperationName("DescribeStream").withProtocolMetadata(protocolMetadata).withMarshaller(new DescribeStreamRequestMarshaller(this.protocolFactory)).withResponseHandler(responseHandler).withErrorResponseHandler(errorResponseHandler).withRequestConfiguration(clientConfiguration).withMetricCollector((MetricCollector)apiCallMetricCollector).withInput(describeStreamRequest));
            CompletableFuture<DescribeStreamResponse> whenCompleted = executeFuture.whenComplete((r, e) -> {
                metricPublishers.forEach((p) -> {
                    p.publish(apiCallMetricCollector.collect());
                });
            });
            executeFuture = CompletableFutureUtils.forwardExceptionTo(whenCompleted, executeFuture);
            return executeFuture;
        } catch (Throwable var10) {
            Throwable t = var10;
            metricPublishers.forEach((p) -> {
                p.publish(apiCallMetricCollector.collect());
            });
            return CompletableFutureUtils.failedFuture(t);
        }
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        return KinesisAsyncClient.super.getShardIterator(getShardIteratorRequest);
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(ListStreamsRequest listStreamsRequest) {
        return KinesisAsyncClient.super.listStreams(listStreamsRequest);
    }

    @Override
    public KinesisAsyncWaiter waiter() {
        return KinesisAsyncClient.super.waiter();
    }

    @Override
    public KinesisServiceClientConfiguration serviceClientConfiguration() {
        return KinesisAsyncClient.super.serviceClientConfiguration();
    }

    @Override
    public String serviceName() {
        return "";
    }

    @Override
    public void close() {

    }

    /*
        Features that aren't supported in DynamoDB Streams
    */

    /*
        Stream Tags
    */

    @Override
    public CompletableFuture<AddTagsToStreamResponse> addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support stream Tags");
    }

    @Override
    public CompletableFuture<AddTagsToStreamResponse> addTagsToStream(Consumer<AddTagsToStreamRequest.Builder> addTagsToStreamRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support stream tags");
    }

    /*
        Configurable Stream Retention
    */

    @Override
    public CompletableFuture<DecreaseStreamRetentionPeriodResponse> decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support configuring stream retention");
    }

    @Override
    public CompletableFuture<DecreaseStreamRetentionPeriodResponse> decreaseStreamRetentionPeriod(Consumer<DecreaseStreamRetentionPeriodRequest.Builder> decreaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support configuring stream retention");
    }

    /*
        Creating and Deleting Streams
    */

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(CreateStreamRequest createStreamRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support creating a stream through the API");
    }

    @Override
    public CompletableFuture<CreateStreamResponse> createStream(Consumer<CreateStreamRequest.Builder> createStreamRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support creating a stream through the API");
    }

    @Override
    public CompletableFuture<DeleteStreamResponse> deleteStream(DeleteStreamRequest deleteStreamRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support deleting a stream through the API");

    }

    @Override
    public CompletableFuture<DeleteStreamResponse> deleteStream(Consumer<DeleteStreamRequest.Builder> deleteStreamRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support deleting a stream through the API");
    }

    /*
        Resource policies
    */

    @Override
    public CompletableFuture<DeleteResourcePolicyResponse> deleteResourcePolicy(DeleteResourcePolicyRequest deleteResourcePolicyRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support stream resource policies");
    }

    @Override
    public CompletableFuture<DeleteResourcePolicyResponse> deleteResourcePolicy(Consumer<DeleteResourcePolicyRequest.Builder> deleteResourcePolicyRequest) {
        throw new UnsupportedOperationException("DynamoDB Streams does not support stream resource policies");
    }

    /*
        For review
    */

    @Override
    public CompletableFuture<DeregisterStreamConsumerResponse> deregisterStreamConsumer(DeregisterStreamConsumerRequest deregisterStreamConsumerRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DeregisterStreamConsumerResponse> deregisterStreamConsumer(Consumer<DeregisterStreamConsumerRequest.Builder> deregisterStreamConsumerRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DescribeLimitsResponse> describeLimits(DescribeLimitsRequest describeLimitsRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DescribeLimitsResponse> describeLimits(Consumer<DescribeLimitsRequest.Builder> describeLimitsRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DescribeLimitsResponse> describeLimits() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumer(DescribeStreamConsumerRequest describeStreamConsumerRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DescribeStreamConsumerResponse> describeStreamConsumer(Consumer<DescribeStreamConsumerRequest.Builder> describeStreamConsumerRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummary(DescribeStreamSummaryRequest describeStreamSummaryRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DescribeStreamSummaryResponse> describeStreamSummary(Consumer<DescribeStreamSummaryRequest.Builder> describeStreamSummaryRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DisableEnhancedMonitoringResponse> disableEnhancedMonitoring(DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<DisableEnhancedMonitoringResponse> disableEnhancedMonitoring(Consumer<DisableEnhancedMonitoringRequest.Builder> disableEnhancedMonitoringRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<EnableEnhancedMonitoringResponse> enableEnhancedMonitoring(EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<EnableEnhancedMonitoringResponse> enableEnhancedMonitoring(Consumer<EnableEnhancedMonitoringRequest.Builder> enableEnhancedMonitoringRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<GetResourcePolicyResponse> getResourcePolicy(GetResourcePolicyRequest getResourcePolicyRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<GetResourcePolicyResponse> getResourcePolicy(Consumer<GetResourcePolicyRequest.Builder> getResourcePolicyRequest) {
        throw new UnsupportedOperationException("");
    }


    @Override
    public CompletableFuture<IncreaseStreamRetentionPeriodResponse> increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<IncreaseStreamRetentionPeriodResponse> increaseStreamRetentionPeriod(Consumer<IncreaseStreamRetentionPeriodRequest.Builder> increaseStreamRetentionPeriodRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest listShardsRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<ListShardsResponse> listShards(Consumer<ListShardsRequest.Builder> listShardsRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<ListStreamConsumersResponse> listStreamConsumers(ListStreamConsumersRequest listStreamConsumersRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public CompletableFuture<ListStreamConsumersResponse> listStreamConsumers(Consumer<ListStreamConsumersRequest.Builder> listStreamConsumersRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ListStreamConsumersPublisher listStreamConsumersPaginator(ListStreamConsumersRequest listStreamConsumersRequest) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public ListStreamConsumersPublisher listStreamConsumersPaginator(Consumer<ListStreamConsumersRequest.Builder> listStreamConsumersRequest) {
        throw new UnsupportedOperationException("");
    }


    @Override
    public ListStreamsPublisher listStreamsPaginator() {
        return KinesisAsyncClient.super.listStreamsPaginator();
    }

    @Override
    public ListStreamsPublisher listStreamsPaginator(ListStreamsRequest listStreamsRequest) {
        return KinesisAsyncClient.super.listStreamsPaginator(listStreamsRequest);
    }

    @Override
    public CompletableFuture<ListTagsForStreamResponse> listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) {
        return KinesisAsyncClient.super.listTagsForStream(listTagsForStreamRequest);
    }

    @Override
    public CompletableFuture<MergeShardsResponse> mergeShards(MergeShardsRequest mergeShardsRequest) {
        return KinesisAsyncClient.super.mergeShards(mergeShardsRequest);
    }

    @Override
    public CompletableFuture<PutRecordResponse> putRecord(PutRecordRequest putRecordRequest) {
        return KinesisAsyncClient.super.putRecord(putRecordRequest);
    }

    @Override
    public CompletableFuture<PutRecordsResponse> putRecords(PutRecordsRequest putRecordsRequest) {
        return KinesisAsyncClient.super.putRecords(putRecordsRequest);
    }

    @Override
    public CompletableFuture<PutResourcePolicyResponse> putResourcePolicy(PutResourcePolicyRequest putResourcePolicyRequest) {
        return KinesisAsyncClient.super.putResourcePolicy(putResourcePolicyRequest);
    }

    @Override
    public CompletableFuture<RegisterStreamConsumerResponse> registerStreamConsumer(RegisterStreamConsumerRequest registerStreamConsumerRequest) {
        return KinesisAsyncClient.super.registerStreamConsumer(registerStreamConsumerRequest);
    }

    @Override
    public CompletableFuture<RemoveTagsFromStreamResponse> removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        return KinesisAsyncClient.super.removeTagsFromStream(removeTagsFromStreamRequest);
    }

    @Override
    public CompletableFuture<SplitShardResponse> splitShard(SplitShardRequest splitShardRequest) {
        return KinesisAsyncClient.super.splitShard(splitShardRequest);
    }

    @Override
    public CompletableFuture<StartStreamEncryptionResponse> startStreamEncryption(StartStreamEncryptionRequest startStreamEncryptionRequest) {
        return KinesisAsyncClient.super.startStreamEncryption(startStreamEncryptionRequest);
    }

    @Override
    public CompletableFuture<StopStreamEncryptionResponse> stopStreamEncryption(StopStreamEncryptionRequest stopStreamEncryptionRequest) {
        return KinesisAsyncClient.super.stopStreamEncryption(stopStreamEncryptionRequest);
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(SubscribeToShardRequest subscribeToShardRequest, SubscribeToShardResponseHandler asyncResponseHandler) {
        return KinesisAsyncClient.super.subscribeToShard(subscribeToShardRequest, asyncResponseHandler);
    }

    @Override
    public CompletableFuture<UpdateShardCountResponse> updateShardCount(UpdateShardCountRequest updateShardCountRequest) {
        return KinesisAsyncClient.super.updateShardCount(updateShardCountRequest);
    }

    @Override
    public CompletableFuture<UpdateStreamModeResponse> updateStreamMode(UpdateStreamModeRequest updateStreamModeRequest) {
        return KinesisAsyncClient.super.updateStreamMode(updateStreamModeRequest);
    }
}
