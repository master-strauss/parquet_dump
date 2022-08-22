package com.marcolotz.db2parquet.adapters;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.core.events.ParquetByteSequenceEvent;
import com.marcolotz.db2parquet.port.EventConsumer;
import com.marcolotz.db2parquet.port.Encryptor;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import java.util.List;

@Value
@Log4j2
public class EncryptionTransformer implements EventConsumer<ParquetByteSequenceEvent> {

    RingBuffer<EncryptedByteSequenceEvent> outputRingBuffer;
    Encryptor encryptor;

    public EventHandler<ParquetByteSequenceEvent>[] getEventHandler() {
        EventHandler<ParquetByteSequenceEvent> eventHandler
                = (event, sequence, endOfBatch)
                -> processEvent(event, sequence);
        return new EventHandler[] { eventHandler };
    }

    private void processEvent(ParquetByteSequenceEvent event, long sequence) {
        log.debug(() -> "Starting encryption of message with sequence number: " + sequence);
        List<FileData> encryptedFileData = encrypt(event.getParquetFiles());

        final long seq = outputRingBuffer.next();
        final EncryptedByteSequenceEvent encryptEvent = outputRingBuffer.get(seq);
        encryptEvent.setEncryptedData(encryptedFileData);
        outputRingBuffer.publish(seq);

        log.debug(() -> "Finished encryption of message with sequence number: " + sequence);
    }

    private List<FileData> encrypt(List<FileData> fileDataToEncrypt)
    {
        // TODO
        //event.getParquetFiles().stream().map(e -> new EncryptedByteSequenceEvent(e.getContents())).collect(Collectors.toList());
        return fileDataToEncrypt;
    }
}
