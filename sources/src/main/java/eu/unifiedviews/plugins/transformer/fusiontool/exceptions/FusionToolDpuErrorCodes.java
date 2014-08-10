package eu.unifiedviews.plugins.transformer.fusiontool.exceptions;

/**
 * Error codes used in Fusion Tool DPU.
 * The purpose of error codes is to easily identify the place in code where an error occurred.
 *
 * @author Jan Michelfeit
 */
public final class FusionToolDpuErrorCodes {
    /** Disable constructor for a utility class. */
    private FusionToolDpuErrorCodes() {
    }

    // CHECKSTYLE:OFF
    public static final int METADATA_LOADING_ERROR = 13;
    public static final int REPOSITORY_ERROR = 12;
    public static final int FILE_OUTPUT_DIRECTORY = 11;
    public static final int FILE_OUTPUT_WRITER_CREATION = 10;
    public static final int FILE_OUTPUT_PARAM = 9;
    public static final int WRITE_CANONICAL_URI_FILE = 8;
    public static final int READ_CANONICAL_URI_FILE = 7;
    public static final int CONFLICT_RESOLUTION = 6;
    public static final int QUERY_QUADS = 5;
    public static final int SEED_SUBJECTS_LOADING_ERROR = 4;
    public static final int INVALID_SEED_RESOURCE_QUERY = 3;
    public static final int COLLECTION_BUFFER_CREATION_ERROR = 2;
    public static final int SAME_AS_LOADING_ERROR = 1;

}
