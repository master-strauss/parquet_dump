package com.marcolotz.db2parquet.port;

import com.marcolotz.db2parquet.core.events.FileData;

public interface DiskWriter {

  void write(FileData fileData);
}
