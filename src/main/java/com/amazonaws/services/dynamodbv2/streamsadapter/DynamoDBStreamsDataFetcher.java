package com.amazonaws.services.dynamodbv2.streamsadapter;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.amazonaws.services.kinesis.model.ChildShard;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.Iterables;

import lombok.Data;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.kinesis.checkpoint.SentinelCheckpoint;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.leases.ShardInfo;
import software.amazon.kinesis.retrieval.DataFetcherResult;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
import software.amazon.kinesis.retrieval.polling.DataFetcher;
import software.amazon.kinesis.retrieval.polling.KinesisDataFetcher;

public class DynamoDBStreamsDataFetcher implements DataFetcher {

    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsDataFetcher.class);

    private String nextIterator;
    private final String shardId;
    private boolean isShardEndReached;
    private boolean isInitialized;
    private String lastKnownSequenceNumber;
    private InitialPositionInStreamExtended initialPositionInStream;

    /**
     *
     * @param shardInfo The shardInfo object.
     */
    public DynamoDBStreamsDataFetcher(ShardInfo shardInfo) {
        this.shardId = shardInfo.shardId();
    }

    /**
     * Get records from the current position in the stream (up to maxRecords).
     *
     * @return list of records of up to maxRecords size
     */
    public DataFetcherResult getRecords() {
        if (!isInitialized) {
            throw new IllegalArgumentException("KinesisDataFetcher.getRecords called before initialization.");
        }

        if (nextIterator != null) {
            try {
                return new DynamoDBStreamsDataFetcher.AdvancingResult(kinesisProxy.get(nextIterator, maxRecords));
            } catch (ResourceNotFoundException e) {
                LOG.info("Caught ResourceNotFoundException when fetching records for shard " + shardId);
                return TERMINAL_RESULT;
            }
        } else {
            LOG.info("Skipping fetching records from Kinesis for shard " + shardId + ": nextIterator is null.");
            return TERMINAL_RESULT;
        }
    }

    final DataFetcherResult TERMINAL_RESULT = new DataFetcherResult() {
        @Override
        public GetRecordsResponse getResult() {
            return GetRecordsResponse.builder()
                    .millisBehindLatest(null)
                    .records(Collections.emptyList())
                    .nextShardIterator(null)
                    .build();
        }

        @Override
        public GetRecordsResponse accept() {
            isShardEndReached = true;
            return getResult();
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }
    };

    @Data
    class AdvancingResult implements DataFetcherResult {

        final GetRecordsResponse result;

        @Override
        public GetRecordsResponse getResult() {
            return result;
        }

        @Override
        public GetRecordsResponse accept() {
            nextIterator = result.nextShardIterator();
            if (!CollectionUtils.isNullOrEmpty(result.records())) {
                lastKnownSequenceNumber = Iterables.getLast(result.records()).sequenceNumber();
            }
            if (nextIterator == null) {
                LOG.info("Reached shard end: nextIterator is null in AdvancingResult.accept for shard " + shardId);

                isShardEndReached = true;
            }
            return getResult();
        }

        @Override
        public boolean isShardEnd() {
            return isShardEndReached;
        }
    }

    /**
     * Initializes this KinesisDataFetcher's iterator based on the checkpointed sequence number.
     * @param initialCheckpoint Current checkpoint sequence number for this shard.
     * @param initialPositionInStream The initialPositionInStream.
     */
    public void initialize(String initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream) {
        LOG.info("Initializing shard " + shardId + " with " + initialCheckpoint);
        advanceIteratorTo(initialCheckpoint, initialPositionInStream);
        isInitialized = true;
    }

    public void initialize(ExtendedSequenceNumber initialCheckpoint, InitialPositionInStreamExtended initialPositionInStream) {
        LOG.info("Initializing shard " + shardId + " with " + initialCheckpoint.sequenceNumber());
        advanceIteratorTo(initialCheckpoint.sequenceNumber(), initialPositionInStream);
        isInitialized = true;
    }

    /**
     * Advances this KinesisDataFetcher's internal iterator to be at the passed-in sequence number.
     *
     * @param sequenceNumber advance the iterator to the record at this sequence number.
     * @param initialPositionInStream The initialPositionInStream.
     */
    public void advanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream) {
        if (sequenceNumber == null) {
            throw new IllegalArgumentException("SequenceNumber should not be null: shardId " + shardId);
        } else if (sequenceNumber.equals(SentinelCheckpoint.LATEST.toString())) {
            nextIterator = getIterator(ShardIteratorType.LATEST.toString());
        } else if (sequenceNumber.equals(SentinelCheckpoint.TRIM_HORIZON.toString())) {
            nextIterator = getIterator(ShardIteratorType.TRIM_HORIZON.toString());
        } else if (sequenceNumber.equals(SentinelCheckpoint.AT_TIMESTAMP.toString())) {
            nextIterator = getIterator(initialPositionInStream.getTimestamp().toString());
        } else if (sequenceNumber.equals(SentinelCheckpoint.SHARD_END.toString())) {
            nextIterator = null;
        } else {
            nextIterator = getIterator(ShardIteratorType.AT_SEQUENCE_NUMBER.toString(), sequenceNumber);
        }
        if (nextIterator == null) {
            LOG.info("Reached shard end: cannot advance iterator for shard " + shardId);
            isShardEndReached = true;
            // TODO: transition to ShuttingDown state on shardend instead to shutdown state for enqueueing this for cleanup
        }
        this.lastKnownSequenceNumber = sequenceNumber;
        this.initialPositionInStream = initialPositionInStream;
    }

    /**
     * @param iteratorType The iteratorType - either AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER.
     * @param sequenceNumber The sequenceNumber.
     *
     * @return iterator or null if we catch a ResourceNotFound exception
     */
    private String getIterator(String iteratorType, String sequenceNumber) {
        String iterator = null;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling getIterator for " + shardId + ", iterator type " + iteratorType
                        + " and sequence number " + sequenceNumber);
            }
            iterator = getIterator(shardId, iteratorType, sequenceNumber);
        } catch (ResourceNotFoundException e) {
            LOG.info("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
        }
        return iterator;
    }

    /**
     * @param iteratorType The iteratorType - either TRIM_HORIZON or LATEST.
     * @return iterator or null if we catch a ResourceNotFound exception
     */
    private String getIterator(String iteratorType) {
        String iterator = null;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Calling getIterator for " + shardId + " and iterator type " + iteratorType);
            }
            iterator = getIterator(shardId, iteratorType);
        } catch (ResourceNotFoundException e) {
            LOG.info("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
        }
        return iterator;
    }

    /**
     * Gets a new iterator from the last known sequence number i.e. the sequence number of the last record from the last
     * getRecords call.
     */
    public void restartIterator() {
        if (StringUtils.isEmpty(lastKnownSequenceNumber) || initialPositionInStream == null) {
            throw new IllegalStateException("Make sure to initialize the KinesisDataFetcher before restarting the iterator.");
        }
        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream);
    }

    @Override
    public void resetIterator(String shardIterator, String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream) {
        nextIterator = getIterator(shardIterator, sequenceNumber);
    }

    @Override
    public GetRecordsResponse getGetRecordsResponse(GetRecordsRequest request) throws Exception {
        return null;
    }

    @Override
    public GetRecordsRequest getGetRecordsRequest(String nextIterator) {
        return null;
    }

    @Override
    public String getNextIterator(GetShardIteratorRequest request) throws ExecutionException, InterruptedException, TimeoutException {
        return "";
    }

    @Override
    public GetRecordsResponse getRecords(@NonNull String nextIterator) {
        return null;
    }

    @Override
    public StreamIdentifier getStreamIdentifier() {
        return null;
    }

    /**
     * @return the shardEndReached
     */
    public boolean isShardEndReached() {
        return isShardEndReached;
    }

    public List<ChildShard> getChildShards() {
        return Collections.emptyList();
    }

    /** Note: This method has package level access for testing purposes.
     * @return nextIterator
     */
    String getNextIterator() {
        return nextIterator;
    }
}
