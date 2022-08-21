package com.marcolotz.db2parquet.port;

public interface EncryptionTransformer {

    // TODO: Check what's the best data format here
    void transform(Object parquetData);
}
