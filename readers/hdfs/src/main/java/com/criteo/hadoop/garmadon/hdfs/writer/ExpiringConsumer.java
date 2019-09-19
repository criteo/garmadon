package com.criteo.hadoop.garmadon.hdfs.writer;

import com.criteo.hadoop.garmadon.reader.Offset;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.TemporalAmount;

/**
 * Consumer exposing an "expired" status (but not auto-closing) after a set idle time or number of messages. When
 * closing, commit the latest processed offset
 *
 * @param <MESSAGE_TYPE>     The actual consumed message type
 */
public class ExpiringConsumer<MESSAGE_TYPE> implements CloseableWriter<MESSAGE_TYPE> {
    private final CloseableWriter<MESSAGE_TYPE> writer;
    private final TemporalAmount expirationDelay;
    private long messagesReceived;
    private long messagesBeforeExpiring;
    private Instant startDate = Instant.now();

    /**
     * @param writer                    The underlying messages writer. Must support receiving null events, which is a
     *                                  no-op
     * @param expirationDelay           Idle delay after which the writer should get closed
     * @param messagesBeforeExpiring    Number of messages to write before expiring
     */
    public ExpiringConsumer(CloseableWriter<MESSAGE_TYPE> writer, TemporalAmount expirationDelay, long messagesBeforeExpiring) {
        this.writer = writer;
        this.expirationDelay = expirationDelay;
        this.messagesBeforeExpiring = messagesBeforeExpiring;
        this.messagesReceived = 0;
    }

    /**
     * Write a given message and update both the "last write" time and the number of messages written
     *
     * @param message   The message to be written
     */
    @Override
    public void write(long timestamp, MESSAGE_TYPE message, Offset offset) throws IOException {
        ++this.messagesReceived;

        this.writer.write(timestamp, message, offset);
    }

    @Override
    public Path close() throws IOException, SQLException {
        return writer.close();
    }

    /**
     * @return  True if the writer is expired because of creation date being too far away in the past, or enough
     *          messages being written
     */
    boolean isExpired() {
        Instant expirationInstant = startDate.plus(expirationDelay);

        return messagesReceived >= messagesBeforeExpiring || Instant.now().isAfter(expirationInstant);
    }
}
