package com.marcolotz.db2parquet.adapter.parquet;

import com.marcolotz.db2parquet.port.ParquetSerializer;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/***
 * Implementation based on:
 * https://github.com/benleov/parquet-resultset/tree/master/src/main/java/parquet/resultset/impl
 *
 * This class is Threadsafe
 */
public class SimpleParquetSerializer implements ParquetSerializer {

  @Override
  @SneakyThrows
  public byte[] convertToParquet(Schema avroSchema, GenericRecord[] avroRecords) {

    InMemoryOutputFile inMemoryOutputFile = new InMemoryOutputFile();

    ParquetWriter parquetWriter = AvroParquetWriter.builder(inMemoryOutputFile)
      .withSchema(avroSchema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build();

    // Note: Since this is an array, sometimes elements can be null. This iterator ignores them.
    for (GenericRecord record : avroRecords) {
      parquetWriter.write(record);
    }

    parquetWriter.close();
    return inMemoryOutputFile.toArray();
  }
}
