package com.marcolotz.db2parquet.adapter;

import com.marcolotz.db2parquet.port.Encryptor;

public class NoEncryption implements Encryptor {

  @Override
  public byte[] encrypt(byte[] input) {
    return input; // identity
  }
}
