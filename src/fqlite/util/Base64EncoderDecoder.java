package fqlite.util;

import java.util.Base64;

/**
 * This class provides utility methods for working with Base64 encoding and decoding.
 */
public class Base64EncoderDecoder {
 
    /**
     * Encodes a string to Base64.
     *
     * @param input The string to be encoded.
     * @return Returns the Base64 encoded string.
     */
    public static String encodeToBase64(String input) {
        byte[] encodedBytes = Base64.getEncoder().encode(input.getBytes());
        return new String(encodedBytes);
    }
 
    /**
     * Decodes a Base64 encoded string.
     *
     * @param input The Base64 encoded string to be decoded.
     * @return Returns the decoded string.
     */
    public static String decodeFromBase64(String input) {
        byte[] decodedBytes = Base64.getDecoder().decode(input.getBytes());
        return new String(decodedBytes);
    }
 
    /**
     * Checks if a string is encoded to Base64.
     *
     * @param input The string to be checked.
     * @return Returns true if the string is Base64 encoded, false otherwise.
     */
    public static boolean isBase64Encoded(String input) {
        try {
            byte[] decodedBytes = Base64.getDecoder().decode(input.getBytes());
            String decodedString = new String(decodedBytes);
            String reencodedString = Base64.getEncoder().encodeToString(decodedString.getBytes());
            return input.equals(reencodedString);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
 

