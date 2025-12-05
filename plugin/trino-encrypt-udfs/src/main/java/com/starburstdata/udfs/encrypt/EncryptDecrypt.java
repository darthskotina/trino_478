package com.starburstdata.udfs.encrypt;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;

/**
 * Trino scalar functions to encrypt / decrypt VARCHAR values
 * using password-based encryption (PBEWithMD5AndDES).
 *
 * This implementation is written for recent Trino releases (4xx)
 * and Java 25. It uses Slice-based signatures as required by the
 * Trino SPI.
 *
 * NOTE: The exact wire format of the encrypted payload is:
 *   base64( salt[8] || ciphertext[...] )
 * where ciphertext is produced by PBEWithMD5AndDES with the same
 * salt and iteration count.
 */
public final class EncryptDecrypt
{
    private static final String ALGORITHM = "PBEWithMD5AndDES";
    private static final int ITERATION_COUNT = 1000;
    private static final int SALT_LENGTH = 8;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private EncryptDecrypt() {}

    @ScalarFunction("encrypt")
    @Description("UDF to encrypt a value with a given password")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice encrypt(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice value,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice passwordSlice)
    {
        if (value == null) {
            return null;
        }
        if (passwordSlice == null) {
            throw new TrinoException(
                    StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    "Password must not be null");
        }

        String plainText = value.toStringUtf8();
        String password = passwordSlice.toStringUtf8();

        try {
            byte[] salt = generateSalt();
            byte[] cipherText = encryptInternal(plainText, password, salt);

            byte[] payload = new byte[salt.length + cipherText.length];
            System.arraycopy(salt, 0, payload, 0, salt.length);
            System.arraycopy(cipherText, 0, payload, salt.length, cipherText.length);

            String encoded = Base64.getEncoder().encodeToString(payload);
            return Slices.utf8Slice(encoded);
        }
        catch (GeneralSecurityException e) {
            throw new TrinoException(
                    StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Failed to encrypt value",
                    e);
        }
    }

    @ScalarFunction("decrypt")
    @Description("UDF to decrypt a value with a given password")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice decrypt(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice encodedSlice,
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice passwordSlice)
    {
        if (encodedSlice == null) {
            return null;
        }
        if (passwordSlice == null) {
            throw new TrinoException(
                    StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    "Password must not be null");
        }

        String encoded = encodedSlice.toStringUtf8();
        String password = passwordSlice.toStringUtf8();

        byte[] payload;
        try {
            payload = Base64.getDecoder().decode(encoded);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(
                    StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    "Input to decrypt() is not valid base64",
                    e);
        }

        if (payload.length <= SALT_LENGTH) {
            throw new TrinoException(
                    StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    "Input to decrypt() is too short");
        }

        byte[] salt = new byte[SALT_LENGTH];
        byte[] cipherText = new byte[payload.length - SALT_LENGTH];
        System.arraycopy(payload, 0, salt, 0, SALT_LENGTH);
        System.arraycopy(payload, SALT_LENGTH, cipherText, 0, cipherText.length);

        try {
            String plainText = decryptInternal(cipherText, password, salt);
            return Slices.utf8Slice(plainText);
        }
        catch (BadPaddingException e) {
            // Wrong password or corrupted ciphertext. The original
            // example plugin returns a human readable message, so we do the same.
            return Slices.utf8Slice("Wrong password for decryption");
        }
        catch (GeneralSecurityException e) {
            throw new TrinoException(
                    StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Failed to decrypt value",
                    e);
        }
    }

    private static byte[] generateSalt()
    {
        byte[] salt = new byte[SALT_LENGTH];
        SECURE_RANDOM.nextBytes(salt);
        return salt;
    }

    private static byte[] encryptInternal(String plainText, String password, byte[] salt)
            throws GeneralSecurityException
    {
        Objects.requireNonNull(plainText, "plainText is null");
        Objects.requireNonNull(password, "password is null");
        Objects.requireNonNull(salt, "salt is null");

        Cipher cipher = initCipher(Cipher.ENCRYPT_MODE, password, salt);
        return cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
    }

    private static String decryptInternal(byte[] cipherText, String password, byte[] salt)
            throws GeneralSecurityException
    {
        Objects.requireNonNull(cipherText, "cipherText is null");
        Objects.requireNonNull(password, "password is null");
        Objects.requireNonNull(salt, "salt is null");

        Cipher cipher = initCipher(Cipher.DECRYPT_MODE, password, salt);
        byte[] plainBytes = cipher.doFinal(cipherText);
        return new String(plainBytes, StandardCharsets.UTF_8);
    }

    private static Cipher initCipher(int mode, String password, byte[] salt)
            throws GeneralSecurityException
    {
        PBEKeySpec keySpec = new PBEKeySpec(password.toCharArray(), salt, ITERATION_COUNT);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM);
        SecretKey secretKey = keyFactory.generateSecret(keySpec);
        PBEParameterSpec paramSpec = new PBEParameterSpec(salt, ITERATION_COUNT);

        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(mode, secretKey, paramSpec);
        return cipher;
    }
}
