package com.amazonaws.services.dynamodbv2.streamsadapter;

import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.KinesisServiceClientConfiguration;
import software.amazon.awssdk.services.kinesis.model.EnhancedMetrics;

import java.util.stream.Collectors;

/**
 * This class provides mapping functions to convert between DynamoDB Streams and Kinesis data models.
 */
class SDKMapper {
    private static final int DYNAMODB_STREAMS_RETENTION_PERIOD_HOURS = 24;

    /**
     * Creates a DescribeStreamRequest for DynamoDB Streams from a Kinesis DescribeStreamRequest.
     *
     * @param describeStreamRequest The Kinesis DescribeStreamRequest.
     * @return A DynamoDB DescribeStreamRequest.
     */
    public static software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest toDynamoStreamsDescribeStreamRequest(final software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest describeStreamRequest) {
        return software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest.builder()
                .streamArn(describeStreamRequest.streamARN())
                .exclusiveStartShardId(describeStreamRequest.exclusiveStartShardId())
                .limit(describeStreamRequest.limit())
                .build();
    }

    /**
     * Creates a DescribeStreamRequest for DynamoDB Streams from a Kinesis DescribeStreamSummaryRequest.
     *
     * @param describeStreamSummaryRequest The Kinesis DescribeStreamRequest.
     * @return A DynamoDB DescribeStreamRequest.
     */
    public static software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest toDynamoStreamsDescribeStreamRequest(final software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest describeStreamSummaryRequest) {
        return software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest.builder()
                .streamArn(describeStreamSummaryRequest.streamARN())
                .build();
    }

    public static software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse toKinesisDescribeStreamSummaryResponse(software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse describeStreamResponse) {
        final software.amazon.awssdk.services.dynamodb.model.StreamDescription streamDescription = describeStreamResponse.streamDescription();

        return software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse.builder()
                .streamDescriptionSummary(software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary.builder()
                    .enhancedMonitoring(new EnhancedMetrics[0])
                    .retentionPeriodHours(DYNAMODB_STREAMS_RETENTION_PERIOD_HOURS)
                    .streamARN(streamDescription.streamArn())
                    .streamCreationTimestamp(streamDescription.creationRequestDateTime())
                    .streamName(streamDescription.tableName())
                    .streamStatus(toKinesisStreamStatus(streamDescription.streamStatus()))
                    .build()
                ).build();
    }

    /**
     * Converts a DynamoDB DescribeStreamResponse to a Kinesis DescribeStreamResponse.
     *
     * @param response The DynamoDB stream description response.
     * @return A Kinesis DescribeStreamResponse based on the DynamoDB stream description.
     */
    public static software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse toKinesisDescribeStreamResponse(final software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse describeStreamResponse) {
        final software.amazon.awssdk.services.dynamodb.model.StreamDescription streamDescription = describeStreamResponse.streamDescription();

        return software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse.builder()
                .streamDescription(software.amazon.awssdk.services.kinesis.model.StreamDescription.builder()
                        .enhancedMonitoring(new EnhancedMetrics[0])
                        .hasMoreShards(streamDescription.lastEvaluatedShardId() == null || streamDescription.lastEvaluatedShardId().isEmpty())
                        .retentionPeriodHours(DYNAMODB_STREAMS_RETENTION_PERIOD_HOURS)
                        .shards(
                                streamDescription.shards().stream().map(dynamoShard -> software.amazon.awssdk.services.kinesis.model.Shard.builder()
                                        .parentShardId(dynamoShard.parentShardId())
                                        .sequenceNumberRange(toKinesisSequenceNumberRange(dynamoShard.sequenceNumberRange()))
                                        .shardId(dynamoShard.shardId())
                                        .build()
                                ).collect(Collectors.toList())
                        )
                        .streamARN(streamDescription.streamArn())
                        .streamCreationTimestamp(streamDescription.creationRequestDateTime())
                        .streamName(streamDescription.tableName())
                        .streamStatus(toKinesisStreamStatus(streamDescription.streamStatus()))
                        .build()
                ).build();
    }

    /**
     * Converts a Kinesis GetRecordsRequest to a DynamoDB GetRecordsRequest.
     *
     * @param getRecordsRequest The Kinesis GetRecordsRequest.
     * @return A corresponding DynamoDB GetRecordsRequest.
     */
    public static software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest toDynamoGetRecordsRequest(final software.amazon.awssdk.services.kinesis.model.GetRecordsRequest getRecordsRequest) {
        return software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest.builder()
                .shardIterator(getRecordsRequest.shardIterator())
                .limit(getRecordsRequest.limit())
                .build();
    }

    /**
     * Converts a DynamoDB GetRecordsResponse to a Kinesis GetRecordsResponse.
     *
     * @param getRecordsResponse The DynamoDB GetRecordsResponse.
     * @return A corresponding Kinesis GetRecordsResponse.
     */
    public static software.amazon.awssdk.services.kinesis.model.GetRecordsResponse toKinesisGetRecordsResponse(final software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse getRecordsResponse) {
        return software.amazon.awssdk.services.kinesis.model.GetRecordsResponse.builder()
                .records(getRecordsResponse.records().stream()
                        .map(record -> software.amazon.awssdk.services.kinesis.model.Record.builder()
                                .data(record.data())
                                .sequenceNumber(record.sequenceNumber())
                                .partitionKey(record.partitionKey())
                                .build())
                        .collect(Collectors.toList()))
                .nextShardIterator(getRecordsResponse.nextShardIterator())
                .millisBehindLatest(getRecordsResponse.millisBehindLatest())
                .build();
    }

    /**
     * Converts a Kinesis GetShardIteratorRequest to a DynamoDB GetShardIteratorRequest.
     *
     * @param getShardIteratorRequest The Kinesis GetShardIteratorRequest.
     * @return A corresponding DynamoDB GetShardIteratorRequest.
     */
    public static software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest toDynamoGetShardIteratorRequest(final software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest getShardIteratorRequest) {
        return software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest.builder()
                .shardId(getShardIteratorRequest.shardId())
                .shardIteratorType(getShardIteratorRequest.shardIteratorType().toString())
                .startingSequenceNumber(getShardIteratorRequest.startingSequenceNumber())
                .timestamp(getShardIteratorRequest.timestamp())
                .build();
    }

    /**
     * Converts a DynamoDB GetShardIteratorResponse to a Kinesis GetShardIteratorResponse.
     *
     * @param getShardIteratorResponse The DynamoDB GetShardIteratorResponse.
     * @return A corresponding Kinesis GetShardIteratorResponse.
     */
    public static software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse toKinesisGetShardIteratorResponse(final GetShardIteratorResponse getShardIteratorResponse) {
        return software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse.builder()
                .shardIterator(getShardIteratorResponse.shardIterator())
                .build();
    }

    /**
     * Converts a Kinesis ListStreamsRequest to a DynamoDB ListStreamsRequest.
     *
     * @param listStreamsRequest The Kinesis ListStreamsRequest.
     * @return A corresponding DynamoDB ListStreamsRequest.
     */
    public static software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest toDynamoListStreamsRequest(final software.amazon.awssdk.services.kinesis.model.ListStreamsRequest listStreamsRequest) {
        return software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest.builder()
                .exclusiveStartStreamName(listStreamsRequest.exclusiveStartStreamName())  // Assuming ARN is derived from name
                .limit(listStreamsRequest.limit())
                .build();
    }

    /**
     * Converts a DynamoDB ListStreamsResponse to a Kinesis ListStreamsResponse.
     *
     * @param listStreamsResponse The DynamoDB ListStreamsResponse.
     * @return A corresponding Kinesis ListStreamsResponse.
     */
    public static software.amazon.awssdk.services.kinesis.model.ListStreamsResponse toKinesisListStreamsResponse(final ListStreamsResponse listStreamsResponse) {
        return software.amazon.awssdk.services.kinesis.model.ListStreamsResponse.builder()
                .streamNames(listStreamsResponse.streams().stream()
                        .map(Stream::streamId)  // Assuming the stream ID maps directly to a Kinesis stream name
                        .collect(Collectors.toList()))
                .hasMoreStreams(listStreamsResponse.hasMoreStreams())
                .build();
    }

    /**
     * Map DynamoDB StreamStatus to Kinesis StreamStatus.
     *
     * @param status DynamoDB StreamStatus.
     * @return Kinesis StreamStatus.
     *
     * Default to {@link software.amazon.awssdk.services.kinesis.model.StreamStatus#UNKNOWN_TO_SDK_VERSION} when the DynamoDB stream is in {@link StreamStatus#DISABLING} or {@link StreamStatus#DISABLED} status.
     * While {@link software.amazon.awssdk.services.kinesis.model.StreamStatus#DELETING} seems like an obvious analog to {@link StreamStatus#DISABLING}, it may not always be safe, so we err on the side of caution.
     */
    private static software.amazon.awssdk.services.kinesis.model.StreamStatus toKinesisStreamStatus(final software.amazon.awssdk.services.dynamodb.model.StreamStatus status) {
        switch (status) {
            case ENABLING:
                return software.amazon.awssdk.services.kinesis.model.StreamStatus.CREATING;
            case ENABLED:
                return software.amazon.awssdk.services.kinesis.model.StreamStatus.ACTIVE;
            default:
                return software.amazon.awssdk.services.kinesis.model.StreamStatus.UNKNOWN_TO_SDK_VERSION;
        }
    }

    /**
     * Converts a DynamoDB SequenceNumberRange to a Kinesis SequenceNumberRange.
     * This method maps the sequence number range used in DynamoDB streams to the
     * corresponding format used in Kinesis streams. This is crucial for ensuring
     * consistency across different AWS services when working with stream-based data.
     *
     * @param sequenceNumberRange The DynamoDB SequenceNumberRange.
     * @return The corresponding Kinesis SequenceNumberRange.
     */
    private static software.amazon.awssdk.services.kinesis.model.SequenceNumberRange toKinesisSequenceNumberRange(final software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange sequenceNumberRange) {
        return software.amazon.awssdk.services.kinesis.model.SequenceNumberRange.builder()
                .startingSequenceNumber(sequenceNumberRange.startingSequenceNumber())
                .endingSequenceNumber(sequenceNumberRange.endingSequenceNumber().orElse(null)) // Handle optional if API supports it
                .build();
    }

    /**
     * Converts DynamoDbStreamsServiceClientConfiguration to KinesisServiceClientConfiguration.
     * This method is crucial for aligning configurations between services when integrating
     * DynamoDB streams with Kinesis, ensuring that connectivity and performance settings
     * are consistent.
     *
     * @param dynamoDbStreamsServiceClientConfiguration The configuration settings used for DynamoDB Streams.
     * @return A corresponding configuration for the Kinesis client.
     */
    public static KinesisServiceClientConfiguration toKinesisServiceClientConfiguration(final DynamoDbStreamsServiceClientConfiguration dynamoDbStreamsServiceClientConfiguration) {
        return KinesisServiceClientConfiguration.builder()
                .httpClient(dynamoDbStreamsServiceClientConfiguration.httpClient()) // Assuming httpClient configurations are transferable
                .maxConcurrency(dynamoDbStreamsServiceClientConfiguration.maxConcurrency()) // Example: Map max concurrency directly
                .retryPolicy(dynamoDbStreamsServiceClientConfiguration.retryPolicy()) // Direct mapping if compatible
                .build();
    }
}
