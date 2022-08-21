package com.marcolotz.db2parquet.adapters.aes128;

import com.marcolotz.db2parquet.port.Encryptor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

/***
 * Fastest AES-128 algorithm, according to:
 * https://medium.com/@gerritjvv/aes-java-encryption-performance-benchmarks-3c2cb19a40e9
 *
 * inspired on:
 * https://github.com/gerritjvv/crypto/tree/368035585ca88ae550c3f8a36c60c6ad46bde5a6/crypto-core/src/main/java/crypto
 *
 * The bottleneck of the data transformation should be in the database Network side and not on CPU usage
 * from encryption.
 */
@Data
@Log4j2
public class Aes128Encryptor implements Encryptor {

    public static final int GCM_IV_LENGTH = 12;
    public static final String AES_GCM_CIPHER_LBL = "AES/GCM/NoPadding";

    private byte[] encryptionKey;
    private AesKey.ExpandedKey ENC_DEFAULT_KEY_128 = AesKey.KeySize.AES_128.genKeysHmacSha(encryptionKey);

    @Override
    @SneakyThrows
    public byte[] encrypt(byte[] input) {
       log.trace(() -> "encrypting " + input.length + " of data");
        return encryptGCM(ENC_DEFAULT_KEY_128, input);
    }

    private byte[] encryptGCM(AesKey.ExpandedKey key, byte[] txt) throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException, NoSuchProviderException {

        byte[] iv = new byte[GCM_IV_LENGTH];
        Aes256SecureRandom.nextBytes(iv);

        final Cipher cipher = Cipher.getInstance(AES_GCM_CIPHER_LBL);
        GCMParameterSpec parameterSpec = new GCMParameterSpec(key.keySize.getKeySizeBits(), iv);

        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key.encKey, "AES"), parameterSpec);
        byte[] cipherText = cipher.doFinal(txt);

        byte[] output = new byte[1 + 1 + iv.length + cipherText.length];
        int i = 0;
        output[i++] = (byte) 0;
        output[i++] = (byte) iv.length;
        System.arraycopy(iv, 0, output, i, iv.length);
        i += iv.length;

        System.arraycopy(cipherText, 0, output, i, cipherText.length);

        return output;
    }

    // Used only for testing and validation ... Probably should move to the test and remove from here
    // However, this is just a PoC and fixing all the dependencies may take time :)
    byte[] decryptGCM(AesKey.ExpandedKey key, byte[] encryptedMessage) throws NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException, InvalidAlgorithmParameterException, NoSuchPaddingException {

        int i = 0;
        int cipherVersion = encryptedMessage[i++];

        int ivLength = encryptedMessage[i++];

        if (ivLength != GCM_IV_LENGTH) { // check input parameter
            throw new IllegalArgumentException("invalid iv length: " + ivLength);
        }

        int ivPos = i;

        byte[] iv = new byte[ivLength];
        System.arraycopy(encryptedMessage, ivPos, iv, 0, ivLength);
        i += ivLength;

        int cipherTextPos = i;
        int cipherTextLen = encryptedMessage.length - cipherTextPos;

        final Cipher cipher = Cipher.getInstance(AES_GCM_CIPHER_LBL);
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key.encKey, "AES"), new GCMParameterSpec(key.keySize.getKeySizeBits(), iv));

        return cipher.doFinal(encryptedMessage, cipherTextPos, cipherTextLen);
    }

}
