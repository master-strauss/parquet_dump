package com.marcolotz.db2parquet.port;

import com.lmax.disruptor.RingBuffer;

public interface JdbcProducer<OUTPUT_FORMAT> {

    void produce(final RingBuffer<OUTPUT_FORMAT> ringBuffer);
}
