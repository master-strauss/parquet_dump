package com.marcolotz.db2parquet.adapters;

import com.lmax.disruptor.RingBuffer;
import com.marcolotz.db2parquet.core.events.ResultSetEvent;
import com.marcolotz.db2parquet.core.interfaces.JdbcWorker;
import com.marcolotz.db2parquet.port.JdbcProducer;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import java.sql.ResultSet;

@Value
@Log4j2
public class BaseJdbcImplementation implements JdbcProducer {

    JdbcWorker jdbcWorker;

    RingBuffer<ResultSetEvent> ringBuffer;

    @Override
    public Thread run() {
        final Runnable producer = () -> produce(ringBuffer);
        Thread thread = new Thread(producer);
        thread.start();
        return thread;
    }

    @Override
    @SneakyThrows
    public void produce(RingBuffer<ResultSetEvent> ringBuffer) {
        log.info(() -> "Setting up JDBC connection");
        while (!jdbcWorker.hasFinishedWork())
        {
            // Loads Data into a chunk
            ResultSet resultSet = jdbcWorker.produceResultSet();
            // Write to ring buffer
            final long seq = ringBuffer.next();
            final ResultSetEvent resultSetEvent = ringBuffer.get(seq);
            resultSetEvent.setResultSet(resultSet);
            ringBuffer.publish(seq);
        }
        log.info(() -> "JDBC worker finished consuming data");
    }
}
