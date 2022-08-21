package com.marcolotz.db2parquet.adapters;

import com.lmax.disruptor.EventHandler;
import com.marcolotz.db2parquet.core.events.EncryptedByteSequenceEvent;
import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.core.interfaces.EventConsumer;
import com.marcolotz.db2parquet.port.DiskWriter;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

import java.util.List;

@Log4j2
@Data
public class DiskConsumer implements EventConsumer {

    private final String outputPath;
    DiskWriter diskWriter;

    long lastFinalizedSequence = 0;
    public void processEvent(final List<FileData> fileData, final long sequence) {
        long totalNumberOfBytes = fileData.stream().mapToLong(file -> file.getContents().length).sum();
        log.debug(() -> "starting writing " + totalNumberOfBytes + " bytes to disk");
        fileData.forEach(file -> diskWriter.write(file.getContents(), outputPath + "/" + file.getFileName()));
        lastFinalizedSequence = sequence;
        log.debug(() -> "completed writing " + totalNumberOfBytes + " bytes to disk");
    }

    @Override
    public EventHandler<EncryptedByteSequenceEvent>[] getEventHandler() {
        EventHandler<EncryptedByteSequenceEvent> eventHandler
                = (event, sequence, endOfBatch)
                -> processEvent(event.getEncryptedData(), sequence);
        return new EventHandler[] { eventHandler };
    }
}
