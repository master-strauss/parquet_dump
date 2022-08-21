package com.marcolotz.db2parquet.port;

import com.marcolotz.db2parquet.core.events.FileData;
import com.marcolotz.db2parquet.core.interfaces.EventConsumer;

import java.util.List;

public interface DiskConsumer extends EventConsumer {

    void processEvent(final List<FileData> bytes, final long sequence);
}
