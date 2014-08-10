package eu.unifiedviews.plugins.transformer.fusiontool.io;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Utility class for file name validation.
 * @author Jan Michelfeit
 */
public final class FileNameSanitizer {
    private static final Pattern PATH_PATTERN = Pattern.compile("/|\\\\");
    private static final int[] ILLEGAL_CHARS = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
            26, 27, 28, 29, 30, 31, 34, 60, 62, 124, 58, 42, 63, 92, 47 };
    
    static {
        Arrays.sort(ILLEGAL_CHARS);
    }

    /**
     * Checks if the given fileName contains characters invalid for a fileName.
     * @param fileName string to check
     * @return true if fileName contains no invalid characters
     */
    public static boolean isFileNameValid(String fileName) {
        for (int i = 0; i < fileName.length(); i++) {
            int c = (int) fileName.charAt(i);
            if (Arrays.binarySearch(ILLEGAL_CHARS, c) >= 0) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Checks if the given fileName contains relative or absolute directory path 
     * (i.e. characters '\' or '/'). 
     * @param fileName string to check
     * @return true if fileName contains path part
     */
    public static boolean containsPath(String fileName) {
        return PATH_PATTERN.matcher(fileName).find();
    }
    
    /** Private constructor for utility class. */
    private FileNameSanitizer() {
        
    }
}
