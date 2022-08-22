package com.marcolotz.db2parquet.core.events;

import com.lmax.disruptor.EventFactory;
import java.util.List;
import lombok.Data;

@Data
public class ParquetByteSequenceEvent {

    public final static EventFactory<ParquetByteSequenceEvent> EVENT_FACTORY = ParquetByteSequenceEvent::new;
    // TODO Check how parquet is serialized
    private List<FileData> parquetFiles;

}
