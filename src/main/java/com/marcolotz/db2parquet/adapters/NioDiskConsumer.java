package com.marcolotz.db2parquet.adapters;

import com.lmax.disruptor.EventHandler;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.port.DiskConsumer;
import com.marcolotz.db2parquet.port.DiskWriter;
import lombok.Data;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import java.util.List;

@Log4j2
@Data
public class NioDiskConsumer implements DiskConsumer {

    long lastFinalizedSequence = 0;
    DiskWriter diskWriter;
    @Override
    public void processEvent(final List<FileData> fileData, final long sequence) {
        long totalNumberOfBytes = fileData.stream().mapToLong(file -> file.getContents().length).sum();
        log.debug(() -> "starting writing " + totalNumberOfBytes + " bytes to disk");
        // TODO: Write to disk
        lastFinalizedSequence = sequence;
        log.debug(() -> "completed writing " + totalNumberOfBytes + " bytes to disk");
    }

    public EventHandler<EncryptedByteSequenceEvent>[] getEventHandler() {
        EventHandler<EncryptedByteSequenceEvent> eventHandler
                = (event, sequence, endOfBatch)
                -> processEvent(event.getEncryptedData(), sequence);
        return new EventHandler[] { eventHandler };
    }
}
