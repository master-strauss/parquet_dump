package com.marcolotz.db2parquet.adapters;

import com.lmax.disruptor.RingBuffer;
import com.marcolotz.db2parquet.adapters.avro.JdbcToAvroWorker;
import com.marcolotz.db2parquet.adapters.avro.ParsedAvroSchema;
import com.marcolotz.db2parquet.core.events.AvroResultSetEvent;
import com.marcolotz.db2parquet.port.JdbcProducer;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import org.apache.avro.generic.GenericRecord;

@Value
@Log4j2
public class BaseJdbcImplementation implements JdbcProducer<AvroResultSetEvent> {

    JdbcToAvroWorker jdbcWorker;

    RingBuffer<AvroResultSetEvent> ringBuffer;

    public Thread run() {
        final Runnable producer = () -> produce(ringBuffer);
        Thread thread = new Thread(producer);
        thread.start();
        return thread;
    }

    @Override
    @SneakyThrows
    public void produce(final RingBuffer<AvroResultSetEvent> ringBuffer){
        log.info(() -> "Starting JDBC producer");
        while (!jdbcWorker.hasFinishedWork())
        {
            // Loads Data into a chunk
            GenericRecord[] records = jdbcWorker.produceAvroRecords();
            ParsedAvroSchema parsedAvroSchema = jdbcWorker.getAvroSchema();
            // Write to ring buffer
            final long seq = ringBuffer.next();
            final AvroResultSetEvent resultSetEvent = ringBuffer.get(seq);
            resultSetEvent.setAvroSchema(parsedAvroSchema.getParsedSchema());
            resultSetEvent.setAvroRecords(records);
            ringBuffer.publish(seq);
        }
        log.info(() -> "JDBC worker finished consuming data");
    }
}
