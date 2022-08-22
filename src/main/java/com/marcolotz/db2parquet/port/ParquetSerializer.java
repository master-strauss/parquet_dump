package com.marcolotz.db2parquet.port;

import com.marcolotz.db2parquet.adapters.avro.ParsedAvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface ParquetSerializer {

    byte[] convertToParquet(Schema avroSchema, GenericRecord[] avroRecords);

}
