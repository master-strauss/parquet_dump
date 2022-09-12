package com.marcolotz.db2parquet.adapter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.marcolotz.db2parquet.port.Encryptor;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName( "When using not encryption" )
class NoEncryptionTest {

  Encryptor encryption = new NoEncryption();

  @DisplayName( "Then the encryption should be an identity mapping" )
  @Test
  void whenEncrypting_thenReturnsInput() {
    // Given
    byte[] b = new byte[2048];
    new Random().nextBytes(b);

    // When
    final byte[] encryptedOutput = encryption.encrypt(b);

    // Then
    assertEquals(b, encryptedOutput);
  }

}