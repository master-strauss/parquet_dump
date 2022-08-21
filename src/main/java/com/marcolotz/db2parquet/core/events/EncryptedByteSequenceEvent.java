package com.marcolotz.db2parquet.core.events;

import com.lmax.disruptor.EventFactory;
import lombok.Data;

import java.util.List;

@Data
public class EncryptedByteSequenceEvent {
    public final static EventFactory<EncryptedByteSequenceEvent> EVENT_FACTORY = EncryptedByteSequenceEvent::new;
    public List<FileData> encryptedData;
}
