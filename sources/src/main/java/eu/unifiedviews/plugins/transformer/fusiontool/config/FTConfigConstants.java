/**
 * 
 */
package eu.unifiedviews.plugins.transformer.fusiontool.config;

import cz.cuni.mff.odcleanstore.fusiontool.config.LDFTConfigConstants;
import org.openrdf.model.URI;
import org.openrdf.rio.ParserConfig;

import java.util.Collection;
import java.util.Set;

/**
 * Global configuration constants.
 * Contains default values and values which cannot be currently set via the configuration file. 
 * @author Jan Michelfeit
 */ 
public final class FTConfigConstants {
    /** Disable constructor for a utility class. */
    private FTConfigConstants() {
    }

    /**
     * Default prefix of named graphs and URIs where query results and metadata in the output are placed.
     */
    public static final String DEFAULT_RESULT_DATA_URI_PREFIX = LDFTConfigConstants.DEFAULT_RESULT_DATA_URI_PREFIX;

    /**
     * Coefficient used in quality computation formula. Value N means that (N+1)
     * sources with score 1 that agree on the result will increase the result
     * quality to 1.
     */
    public static final double AGREE_COEFFICIENT = LDFTConfigConstants.AGREE_COEFFICIENT;

    /**
     * Graph score used if none is given in the input.
     */
    public static final double SCORE_IF_UNKNOWN = LDFTConfigConstants.SCORE_IF_UNKNOWN;

    /**
     * Weight of the publisher score.
     */
    public static final double PUBLISHER_SCORE_WEIGHT = LDFTConfigConstants.PUBLISHER_SCORE_WEIGHT;

    /**
     * Difference between two dates when their distance is equal to MAX_DISTANCE in seconds.
     * 31622400 s ~ 366 days
     */
    public static final long MAX_DATE_DIFFERENCE = LDFTConfigConstants.MAX_DATE_DIFFERENCE;

    /**
     * Maximum number of values in a generated argument for the "?var IN (...)" SPARQL construct .
     */
    public static final int MAX_QUERY_LIST_LENGTH = LDFTConfigConstants.MAX_QUERY_LIST_LENGTH;

    /**
     * Set of default preferred canonical URIs.
     */
    public static final Collection<String> DEFAULT_PREFERRED_CANONICAL_URIS = LDFTConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS;

    public static final boolean OUTPUT_MAPPED_SUBJECTS_ONLY = false;

    public static final ParserConfig DEFAULT_FILE_PARSER_CONFIG = LDFTConfigConstants.DEFAULT_FILE_PARSER_CONFIG;

    public static final int DEFAULT_QUERY_TIMEOUT = LDFTConfigConstants.DEFAULT_QUERY_TIMEOUT;
    public static final float MAX_FREE_MEMORY_USAGE = LDFTConfigConstants.MAX_FREE_MEMORY_USAGE;
    public static final Set<URI> SAME_AS_LINK_TYPES = LDFTConfigConstants.SAME_AS_LINK_TYPES;
    public static final String CANONICAL_URI_FILE_NAME = "canonicalUris.txt";
    public static final boolean ENABLE_FILE_CACHE = false;

}
