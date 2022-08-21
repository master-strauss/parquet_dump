package com.marcolotz.db2parquet.core.events;

import com.lmax.disruptor.EventFactory;
import lombok.Data;

import java.util.List;

@Data
public class ParquetByteSequenceEvent {

    public final static EventFactory<ParquetByteSequenceEvent> EVENT_FACTORY = ParquetByteSequenceEvent::new;
    // TODO Check how parquet is serialized
    private List<FileData> parquetFiles;

}
