package com.marcolotz.db2parquet.port;

public interface DiskWriter {

    void write(byte[] content, String path);
}
