package com.marcolotz.db2parquet.port;

import com.lmax.disruptor.RingBuffer;
import com.marcolotz.db2parquet.core.events.ResultSetEvent;

public interface JdbcProducer  {

    public Thread run();
    void produce(final RingBuffer<ResultSetEvent> ringBuffer);
}
