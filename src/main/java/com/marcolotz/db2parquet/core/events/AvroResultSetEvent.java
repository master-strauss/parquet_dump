package com.marcolotz.db2parquet.core.events;

import com.lmax.disruptor.EventFactory;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

@Data
public class AvroResultSetEvent {
    public final static EventFactory EVENT_FACTORY = AvroResultSetEvent::new;
    private Schema avroSchema;
    private GenericRecord[] avroRecords;
}
