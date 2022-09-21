package com.marcolotz.db2parquet.adapter;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.core.events.AvroResultSetEvent;
import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.core.events.ParquetByteSequenceEvent;
import com.marcolotz.db2parquet.port.ParquetSerializer;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class ParquetTransformer extends BaseTransformer<AvroResultSetEvent, ParquetByteSequenceEvent> {

  private final ParquetSerializer parquetSerializer;

  public ParquetTransformer(final ParquetSerializer parquetSerializer,
    final Disruptor<AvroResultSetEvent> inboundDisruptor,
    final Disruptor<ParquetByteSequenceEvent> outboundDisruptor) {
    super(inboundDisruptor, outboundDisruptor);
    this.parquetSerializer = parquetSerializer;
  }

  private byte[] convertToParquet(Schema avroSchema, GenericRecord[] avroRecords) {
    return parquetSerializer.convertToParquet(avroSchema, avroRecords);
  }

  private FileData convertToFileData(final byte[] parquetBytes) {
    return new FileData("Parquet_file_" + UUID.randomUUID(), parquetBytes);
  }

  @Override
  public void transform(AvroResultSetEvent avroResultSetEvent) {
    final FileData fileData = convertToFileData(
      convertToParquet(avroResultSetEvent.getAvroSchema(), avroResultSetEvent.getAvroRecords()));
    final RingBuffer<ParquetByteSequenceEvent> ringBuffer = outputDisruptor.getRingBuffer();
    final long seq = ringBuffer.next();
    final ParquetByteSequenceEvent encryptEvent = ringBuffer.get(seq);
    encryptEvent.setParquetFile(fileData);
    ringBuffer.publish(seq);
  }
}
