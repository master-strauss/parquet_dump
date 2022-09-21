package com.marcolotz.db2parquet.adapter;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.core.events.AvroResultSetEvent;
import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.core.events.ParquetByteSequenceEvent;
import com.marcolotz.db2parquet.port.EventTransformer;
import com.marcolotz.db2parquet.port.ParquetSerializer;
import java.util.UUID;
import lombok.Value;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

@Value
public class ParquetTransformer implements EventTransformer<AvroResultSetEvent, FileData> {

  ParquetSerializer parquetSerializer;

  Disruptor<ParquetByteSequenceEvent> outputDisruptor;
  Disruptor<AvroResultSetEvent> inboundDisruptor;

  public ParquetTransformer(final ParquetSerializer parquetSerializer,
    final Disruptor<AvroResultSetEvent> inboundDisruptor,
    final Disruptor<ParquetByteSequenceEvent> outboundDisruptor) {
    this.parquetSerializer = parquetSerializer;
    this.outputDisruptor = outboundDisruptor;
    this.inboundDisruptor = inboundDisruptor;
    inboundDisruptor.handleEventsWith(getEventHandler());
  }

  @Override
  public EventHandler<AvroResultSetEvent>[] getEventHandler() {
    EventHandler<AvroResultSetEvent> eventHandler
      = (event, sequence, endOfBatch)
      -> transform(convertToFileData(convertToParquet(event.getAvroSchema(), event.getAvroRecords())));
    return new EventHandler[]{eventHandler};
  }

  private byte[] convertToParquet(Schema avroSchema, GenericRecord[] avroRecords) {
    return parquetSerializer.convertToParquet(avroSchema, avroRecords);
  }

  @Override
  public void transform(final FileData fileData) {
    final RingBuffer<ParquetByteSequenceEvent> ringBuffer = outputDisruptor.getRingBuffer();
    final long seq = ringBuffer.next();
    final ParquetByteSequenceEvent encryptEvent = ringBuffer.get(seq);
    encryptEvent.setParquetFile(fileData);
    ringBuffer.publish(seq);
  }

  private FileData convertToFileData(final byte[] parquetBytes) {
    return new FileData("Parquet_file_" + UUID.randomUUID(), parquetBytes);
  }

  // TODO: This could be in an abstract class...
  public boolean finishedProcessingAllMessages() {
    // All messages in the input have been transformed to the output
    return inboundDisruptor.getRingBuffer().getCursor() == outputDisruptor.getRingBuffer()
      .getCursor();
  }
}
