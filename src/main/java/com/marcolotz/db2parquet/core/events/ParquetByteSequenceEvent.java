package com.marcolotz.db2parquet.core.events;

import com.lmax.disruptor.EventFactory;
import lombok.Data;

@Data
public class ParquetByteSequenceEvent {

  public final static EventFactory<ParquetByteSequenceEvent> EVENT_FACTORY = ParquetByteSequenceEvent::new;
  private FileData parquetFile;

}
