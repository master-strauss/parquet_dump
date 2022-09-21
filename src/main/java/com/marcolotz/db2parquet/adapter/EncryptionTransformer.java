package com.marcolotz.db2parquet.adapter;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.core.events.ParquetByteSequenceEvent;
import com.marcolotz.db2parquet.port.Encryptor;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class EncryptionTransformer extends BaseTransformer<ParquetByteSequenceEvent, EncryptedByteSequenceEvent> {

  private final Encryptor encryptor;

  public EncryptionTransformer(final Encryptor encryptor,
    final Disruptor<ParquetByteSequenceEvent> inputboundDisruptor,
    final Disruptor<EncryptedByteSequenceEvent> outputDisruptor) {
    super(inputboundDisruptor, outputDisruptor);
    this.encryptor = encryptor;
  }

  private FileData toEncryptedFile(final FileData notEncryptedFile) {
    return new FileData(notEncryptedFile.getFileName(),
      encryptor.encrypt(notEncryptedFile.getContents()));
  }

  private FileData encrypt(FileData fileDataToEncrypt) {
    return toEncryptedFile((fileDataToEncrypt));
  }

  @Override
  public void transform(ParquetByteSequenceEvent parquetByteSequenceEvent) {
    FileData encryptedFileData = encrypt(parquetByteSequenceEvent.getParquetFile());
    final RingBuffer<EncryptedByteSequenceEvent> ringBuffer = outputDisruptor.getRingBuffer();
    final long seq = ringBuffer.next();
    final EncryptedByteSequenceEvent encryptEvent = ringBuffer.get(seq);
    encryptEvent.setEncryptedData(encryptedFileData);
    ringBuffer.publish(seq);
  }
}
