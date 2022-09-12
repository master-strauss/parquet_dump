package com.marcolotz.db2parquet.adapter.aes128;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName( "When using AES128 GCM" )
class Aes128EncryptorTest {

  static String message = "You're tuned to Dawn FM\n" +
    "The middle of nowhere on your dial\n" +
    "So sit back and unpack\n" +
    "You may be here awhile\n" +
    "Now that all future plans have been postponed\n" +
    "And it's time to look back on the things you thought you owned\n" +
    "Do you remember them well?\n" +
    "Were you high or just stoned?\n" +
    "And how many grudges did you take to your grave?\n" +
    "When you weren't liked or followed, how did you behave?\n" +
    "Was it often a dissonant chord you were strumming?\n" +
    "Were you ever in tune with the song life was humming?\n" +
    "If pain's living on when your body's long gone\n" +
    "And your phantom regret hasn't let it go yet\n" +
    "You may not have died in the way that you must\n" +
    "All specters are haunted by their own lack of trust\n" +
    "When you're all out of time, there's nothing but space\n" +
    "No hunting, no gathering\n" +
    "No nations, no race\n" +
    "And Heaven is closer than those tears on your face\n" +
    "When the purple rain falls\n" +
    "We're all bathed in its grace\n" +
    "Heaven's for those who let go of regret\n" +
    "And you have to wait here when you're not all there yet\n" +
    "But you could be there by the end of this song\n" +
    "Where The Weeknd's so good and he plays all week long\n" +
    "Bang a gong, get it on\n" +
    "And if your broken heart's heavy when you step on the scale\n" +
    "You'll be lighter than air when they pull back the veil\n" +
    "Consider the flowers, they don't try to look right\n" +
    "They just open their petals and turn to the light\n" +
    "Are you listening real close?\n" +
    "Heaven's not that, it's this\n" +
    "It's the depth of this moment\n" +
    "You don't reach for bliss\n" +
    "God knows life is chaos\n" +
    "But He made one thing true\n" +
    "You gotta unwind your mind\n" +
    "Train your soul to align\n" +
    "And dance 'til you find that divine boogaloo\n" +
    "In other words\n" +
    "You gotta be Heaven to see Heaven\n" +
    "May peace be with you";
  Aes128Encryptor encryption = new Aes128Encryptor("ThisIsAnEncryptionKey".getBytes());

  @DisplayName( "Then the encryption should AES 128 GCM and should be possible to decrypt" )
  @Test
  void whenEncrypting_thenReturnsEncryptedOutput() {
    // Given
    byte[] b = message.getBytes();

    // When
    final byte[] encryptedOutput = encryption.encrypt(b);
    final byte[] descryptedOutput = encryption.decrypt(encryptedOutput);

    // Then
    assertArrayEquals(b, descryptedOutput);
    final String decodedMessage = new String(descryptedOutput);
    assertEquals(message, decodedMessage);
  }
}