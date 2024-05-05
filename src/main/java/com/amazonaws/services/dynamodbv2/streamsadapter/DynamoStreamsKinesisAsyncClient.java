package com.amazonaws.services.dynamodbv2.streamsadapter;

import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.waiters.KinesisAsyncWaiter;

import java.util.concurrent.CompletableFuture;

public final class DynamoStreamsKinesisAsyncClient implements KinesisAsyncClient {

    DynamoDbStreamsAsyncClient dynamoStreamsClient;

    public DynamoStreamsKinesisAsyncClient(DynamoDbStreamsAsyncClient dynamoStreamsClient) {
        this.dynamoStreamsClient = dynamoStreamsClient;
    }

    @Override
    public CompletableFuture<DescribeStreamResponse> describeStream(DescribeStreamRequest describeStreamRequest) {
        return dynamoStreamsClient
                .describeStream(SDKMapper.toDynamoStreamsDescribeStreamRequest(describeStreamRequest))
                .thenApply(SDKMapper::toKinesisDescribeStreamResponse);
    }

    @Override
    public CompletableFuture<GetRecordsResponse> getRecords(GetRecordsRequest getRecordsRequest) {
        return dynamoStreamsClient
                .getRecords(SDKMapper.toDynamoGetRecordsRequest(getRecordsRequest))
                .thenApply(SDKMapper::toKinesisGetRecordsResponse);
    }

    @Override
    public CompletableFuture<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        return dynamoStreamsClient
                .getShardIterator(SDKMapper.toDynamoGetShardIteratorRequest(getShardIteratorRequest))
                .thenApply(SDKMapper::toKinesisGetShardIteratorResponse);
    }

    @Override
    public CompletableFuture<ListStreamsResponse> listStreams(ListStreamsRequest listStreamsRequest) {
        return dynamoStreamsClient
                .listStreams(SDKMapper.toDynamoListStreamsRequest(listStreamsRequest))
                .thenApply(SDKMapper::toKinesisListStreamsResponse);
    }

    @Override
    public KinesisAsyncWaiter waiter() {
        return KinesisAsyncWaiter.builder()
                .client(this)
                .build();
    }

    @Override
    public KinesisServiceClientConfiguration serviceClientConfiguration() {
        return SDKMapper.toKinesisServiceClientConfiguration(dynamoStreamsClient.serviceClientConfiguration());
    }

    @Override
    public String serviceName() {
        return dynamoStreamsClient.serviceName();
    }

    @Override
    public void close() {
        dynamoStreamsClient.close();
    }
}
