package com.marcolotz.db2parquet.port;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface ParquetSerializer {

    byte[] convertToParquet(Schema avroSchema, GenericRecord[] avroRecords);

}
