package com.marcolotz.db2parquet.port;

public interface Encryptor {

  byte[] encrypt(byte[] input);
}
