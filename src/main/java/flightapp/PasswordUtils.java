package flightapp;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.SecureRandom;
import java.util.Arrays;

/**
 * A collection of utility methods to help with managing passwords
 */
public class PasswordUtils {
  /**
   * Generates a cryptographically-secure salted password.
   */
  public static byte[] saltAndHashPassword(String password) {
    byte[] salt = generateSalt();
    byte[] saltedHash = hashWithSalt(password, salt);
    byte[] saltAndSaltedHash = new byte[SALT_LENGTH_BYTES + saltedHash.length];

    // Add each byte from salt
    for (int i = 0; i < SALT_LENGTH_BYTES; i++) {
      saltAndSaltedHash[i] = salt[i];
    }
    // Add each byte from saltedHash
    for (int i = 0; i < saltedHash.length; i++) {
      saltAndSaltedHash[i + SALT_LENGTH_BYTES] = saltedHash[i];
    }
    return saltAndSaltedHash;
  }

  /**
   * Verifies whether the plaintext password can be hashed to provided salted hashed password.
   */
  public static boolean plaintextMatchesSaltedHash(String plaintext, byte[] saltedHashed) {

    byte[] salt = new byte[SALT_LENGTH_BYTES];
    byte[] saltedHash = new byte[saltedHashed.length - SALT_LENGTH_BYTES];

    // Add first 16 bytes to salt
    for (int i = 0; i < SALT_LENGTH_BYTES; i++) {
      salt[i] = saltedHashed[i];
    }
    // Add remaining bytes to saltedHash
    for (int i = 0; i < saltedHashed.length - SALT_LENGTH_BYTES; i++) {
      saltedHash[i] = saltedHashed[i + SALT_LENGTH_BYTES];
    }

    byte[] plaintextSaltedHash = hashWithSalt(plaintext, salt);
    return Arrays.equals(saltedHash, plaintextSaltedHash);
  }
  
  // Password hashing parameter constants.
  private static final int HASH_STRENGTH = 65536;
  private static final int KEY_LENGTH_BYTES = 128;
  private static final int SALT_LENGTH_BYTES = 16;

  /**
   * Generate a small bit of randomness to serve as a password "salt"
   */
  static byte[] generateSalt() {
    SecureRandom secureRandom = new SecureRandom();
    byte[] salt = new byte[SALT_LENGTH_BYTES];
    secureRandom.nextBytes(salt);
    return salt;
  }

  /**
   * Uses the provided salt to generate a cryptographically-secure hash of the provided password.
   * The resultant byte array will be KEY_LENGTH_BYTES bytes long.
   */
  static byte[] hashWithSalt(String password, byte[] salt)
    throws IllegalStateException {
    // Specify the hash parameters, including the salt
    KeySpec spec = new PBEKeySpec(password.toCharArray(), salt,
                                  HASH_STRENGTH, KEY_LENGTH_BYTES * 8 /* length in bits */);

    // Hash the whole thing
    SecretKeyFactory factory = null;
    byte[] hash = null; 
    try {
      factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
      hash = factory.generateSecret(spec).getEncoded();
      return hash;
    } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
      throw new IllegalStateException();
    }
  }
}
