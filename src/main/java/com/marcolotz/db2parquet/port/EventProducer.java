package com.marcolotz.db2parquet.port;

import com.lmax.disruptor.RingBuffer;

/**
 * Producer that produces event for ring buffer.
 */
public interface EventProducer<EVENTTYPE> {

    /**
     * Start the producer that would start producing the values.
     * @param ringBuffer
     * @param count
     */
    void startProducing(final RingBuffer<EVENTTYPE> ringBuffer, final int count);
}
