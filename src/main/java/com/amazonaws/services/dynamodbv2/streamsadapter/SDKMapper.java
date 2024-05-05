package com.amazonaws.services.dynamodbv2.streamsadapter;

import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SDKMapper {
    private static final int DYNAMODB_STREAMS_RETENTION_PERIOD_HOURS = 24;

    public static software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse toKinesisDescribeStreamResponse(final DescribeStreamResponse response) {
        final StreamDescription streamDescription = response.streamDescription();

        return software.amazon.awssdk.services.kinesis.model.StreamDescription.builder()
                .enhancedMonitoring(new EnhancedMetrics[0])
                .hasMoreShards(streamDescription.lastEvaluatedShardId() == null || streamDescription.lastEvaluatedShardId().isEmpty())
                .retentionPeriodHours(DYNAMODB_STREAMS_RETENTION_PERIOD_HOURS)
                .shards(
                        streamDescription.shards().stream().map(dynamoShard -> Shard.builder()
                                .parentShardId(dynamoShard.parentShardId())
                                .sequenceNumberRange(dynamoShard.sequenceNumberRange())
                                .shardId(dynamoShard.shardId())
                                .build()
                        ).collect(Collectors.toList())
                )
                .streamARN(streamDescription.streamArn())
                .streamCreationTimestamp(streamDescription.creationRequestDateTime())
                .streamName(streamDescription.tableName())
                .streamStatus("ENABLED")
                .build(); // TODO: Map out the other statii
    }

    public static DescribeStreamRequest toDynamoStreamsDescribeStreamRequest(final software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest request) {
        return DescribeStreamRequest.builder()
                .streamArn(request.streamARN())
                .exclusiveStartShardId(request.exclusiveStartShardId())
                .limit(request.limit())
                .build();
    }

    public static Consumer<software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest.Builder> toDynamoGetRecordsRequest(GetRecordsRequest getRecordsRequest) {
        return null;
    }

    public static GetRecordsResponse toKinesisGetRecordsResponse(software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse getRecordsResponse) {
        return null;
    }

    public static Consumer<software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest.Builder> toDynamoGetShardIteratorRequest(GetShardIteratorRequest getShardIteratorRequest) {
        return null;
    }

    public static GetShardIteratorResponse toKinesisGetShardIteratorResponse(software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse getShardIteratorResponse) {
        return null;
    }

    public static Consumer<software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest.Builder> toDynamoListStreamsRequest(ListStreamsRequest listStreamsRequest) {
        return null;
    }

    public static ListStreamsResponse toKinesisListStreamsResponse(software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse listStreamsResponse) {
        return null;
    }

    public static KinesisServiceClientConfiguration toKinesisServiceClientConfiguration(DynamoDbStreamsServiceClientConfiguration dynamoDbStreamsServiceClientConfiguration) {
        return null;
    }
}
