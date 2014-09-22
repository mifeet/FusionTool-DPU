package eu.unifiedviews.plugins.transformer.fusiontool.config;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationErrorStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumCardinality;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.InvalidInputException;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;

import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class ConfigReaderTest {

    private static final URI FB_LONGITUDE = new URIImpl("http://rdf.freebase.com/ns/location.geocode.longitude");
    private static final URI FB_LATITUDE = new URIImpl("http://rdf.freebase.com/ns/location.geocode.latitude");

    @Test
    public void parsesMinimalConfigFile() throws Exception {
        // Arrange
        String configString = getResourceString("/config/sample-config-minimal.xml");

        // Act
        ConfigContainer config = ConfigReader.parseConfigXml(configString);

        // Assert
        assertThat(config.getDefaultResolutionStrategy(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getResolutionFunctionName(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getAggregationErrorStrategy(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getCardinality(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getDependsOn(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getParams(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getParams().size(), equalTo(0));
        assertThat(config.getEnableFileCache(), equalTo(false));
        assertThat(config.getMaxOutputTriples(), nullValue());
        assertThat(config.getPrefixes(), equalTo(Collections.<String, String>emptyMap()));
        assertThat(config.getRequiredClassOfProcessedResources(), nullValue());
        assertThat(config.getPropertyResolutionStrategies(), equalTo(Collections.<URI, ResolutionStrategy>emptyMap()));
        assertThat(config.isLocalCopyProcessing(), equalTo(true));

        assertThat(config.getMaxDateDifference(), equalTo(FTConfigConstants.MAX_DATE_DIFFERENCE));
        assertThat(config.getOutputMappedSubjectsOnly(), equalTo(false));
        assertThat(config.getPreferredCanonicalURIs(), equalTo(FTConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS));
        assertThat(config.getResultDataURIPrefix(), notNullValue());
        assertThat(config.getPublisherScoreWeight(), equalTo(FTConfigConstants.PUBLISHER_SCORE_WEIGHT));
        assertThat(config.getAgreeCoefficient(), equalTo(FTConfigConstants.AGREE_COEFFICIENT));
        assertThat(config.getQueryTimeout(), equalTo(FTConfigConstants.DEFAULT_QUERY_TIMEOUT));
        assertThat(config.getScoreIfUnknown(), equalTo(FTConfigConstants.SCORE_IF_UNKNOWN));
        assertThat(config.isProfilingOn(), equalTo(false));
        assertThat(config.getMaxFreeMemoryUsage(), equalTo(FTConfigConstants.MAX_FREE_MEMORY_USAGE));
        assertThat(config.getMemoryLimit(), equalTo(null));
        assertThat(config.getParserConfig(), equalTo(FTConfigConstants.DEFAULT_FILE_PARSER_CONFIG));

        assertThat(config.getCanonicalURIsFileName(), is(FTConfigConstants.CANONICAL_URI_FILE_NAME));
        assertThat(config.getSameAsLinkTypes(), is(FTConfigConstants.SAME_AS_LINK_TYPES));
        assertThat(config.getWriteMetadata(), is(FTConfigConstants.WRITE_METADATA));
        assertThat(config.getDataGraphSymbolicName(), is(FTConfigConstants.DEFAULT_DATA_GRAPH_NAME));
        assertThat(config.getMetadataGraphSymbolicName(), is(FTConfigConstants.DEFAULT_METADATA_GRAPH_NAME));
    }

    @Test
    public void parsesFullConfigFile() throws Exception {
        // Arrange
        String configString = getResourceString("/config/sample-config-full.xml");

        // Act
        ConfigContainer config = ConfigReader.parseConfigXml(configString);

        // Assert
        Map<String, String> expectedPrefixes = ImmutableMap.of(
                "rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "rdfs", "http://www.w3.org/2000/01/rdf-schema#",
                "fb", "http://rdf.freebase.com/ns/");
        assertThat(config.getPrefixes(), equalTo(expectedPrefixes));

        assertThat(config.getMaxOutputTriples(), nullValue());
        assertThat(config.getEnableFileCache(), equalTo(false));
        assertThat(config.isLocalCopyProcessing(), equalTo(true));
        assertThat(config.getRequiredClassOfProcessedResources(), equalTo((URI) new URIImpl("http://schema.org/PostalAddress")));

        assertThat(config.getDefaultResolutionStrategy(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getResolutionFunctionName(), equalTo("ALL"));
        assertThat(config.getDefaultResolutionStrategy().getAggregationErrorStrategy(), equalTo(EnumAggregationErrorStrategy.RETURN_ALL));
        assertThat(config.getDefaultResolutionStrategy().getCardinality(), equalTo(EnumCardinality.MANYVALUED));

        assertThat(config.getPropertyResolutionStrategies().size(), equalTo(3));
        assertThat(config.getPropertyResolutionStrategies().get(RDFS.LABEL).getResolutionFunctionName(), equalTo("BEST"));
        assertThat(config.getPropertyResolutionStrategies().get(RDFS.LABEL).getDependsOn(), nullValue());
        assertThat(config.getPropertyResolutionStrategies().get(RDFS.LABEL).getParams(), equalTo((Map<String, String>) ImmutableMap.of("name", "value")));
        assertThat(config.getPropertyResolutionStrategies().get(FB_LONGITUDE).getResolutionFunctionName(), equalTo("AVG"));
        assertThat(config.getPropertyResolutionStrategies().get(FB_LONGITUDE).getDependsOn(), equalTo(FB_LATITUDE));
        assertThat(config.getPropertyResolutionStrategies().get(FB_LATITUDE).getResolutionFunctionName(), equalTo("AVG"));
        assertThat(config.getPropertyResolutionStrategies().get(FB_LATITUDE).getParams(), equalTo((Map<String, String>) ImmutableMap.<String, String>of()));
        assertThat(config.getPropertyResolutionStrategies().get(FB_LATITUDE).getDependsOn(), equalTo(FB_LATITUDE));

        assertThat(config.getMaxDateDifference(), equalTo(FTConfigConstants.MAX_DATE_DIFFERENCE));
        assertThat(config.getOutputMappedSubjectsOnly(), equalTo(false));
        assertThat(config.getPreferredCanonicalURIs(), equalTo(FTConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS));
        assertThat(config.getResultDataURIPrefix(), notNullValue());
        assertThat(config.getPublisherScoreWeight(), equalTo(FTConfigConstants.PUBLISHER_SCORE_WEIGHT));
        assertThat(config.getAgreeCoefficient(), equalTo(FTConfigConstants.AGREE_COEFFICIENT));
        assertThat(config.getQueryTimeout(), equalTo(FTConfigConstants.DEFAULT_QUERY_TIMEOUT));
        assertThat(config.getScoreIfUnknown(), equalTo(FTConfigConstants.SCORE_IF_UNKNOWN));
        assertThat(config.isProfilingOn(), equalTo(false));
        assertThat(config.getMaxFreeMemoryUsage(), equalTo(FTConfigConstants.MAX_FREE_MEMORY_USAGE));
        assertThat(config.getMemoryLimit(), equalTo(null));
        assertThat(config.getParserConfig(), equalTo(FTConfigConstants.DEFAULT_FILE_PARSER_CONFIG));

        assertThat(config.getCanonicalURIsFileName(), is(FTConfigConstants.CANONICAL_URI_FILE_NAME));
        assertThat(config.getSameAsLinkTypes(), is(FTConfigConstants.SAME_AS_LINK_TYPES));
        assertThat(config.getWriteMetadata(), is(FTConfigConstants.WRITE_METADATA));
        assertThat(config.getDataGraphSymbolicName(), is(FTConfigConstants.DEFAULT_DATA_GRAPH_NAME));
        assertThat(config.getMetadataGraphSymbolicName(), is(FTConfigConstants.DEFAULT_METADATA_GRAPH_NAME));
    }

    @Test
    public void parsesEmptyString() throws Exception {
        ConfigContainer config = ConfigReader.parseConfigXml("");
        // Assert
        assertThat(config.getDefaultResolutionStrategy(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getResolutionFunctionName(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getAggregationErrorStrategy(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getCardinality(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getDependsOn(), nullValue());
        assertThat(config.getDefaultResolutionStrategy().getParams(), notNullValue());
        assertThat(config.getDefaultResolutionStrategy().getParams().size(), equalTo(0));
        assertThat(config.getEnableFileCache(), equalTo(false));
        assertThat(config.getMaxOutputTriples(), nullValue());
        assertThat(config.getPrefixes(), equalTo(Collections.<String, String>emptyMap()));
        assertThat(config.getRequiredClassOfProcessedResources(), nullValue());
        assertThat(config.getPropertyResolutionStrategies(), equalTo(Collections.<URI, ResolutionStrategy>emptyMap()));
        assertThat(config.isLocalCopyProcessing(), equalTo(true));

        assertThat(config.getMaxDateDifference(), equalTo(FTConfigConstants.MAX_DATE_DIFFERENCE));
        assertThat(config.getOutputMappedSubjectsOnly(), equalTo(false));
        assertThat(config.getPreferredCanonicalURIs(), equalTo(FTConfigConstants.DEFAULT_PREFERRED_CANONICAL_URIS));
        assertThat(config.getResultDataURIPrefix(), notNullValue());
        assertThat(config.getPublisherScoreWeight(), equalTo(FTConfigConstants.PUBLISHER_SCORE_WEIGHT));
        assertThat(config.getAgreeCoefficient(), equalTo(FTConfigConstants.AGREE_COEFFICIENT));
        assertThat(config.getQueryTimeout(), equalTo(FTConfigConstants.DEFAULT_QUERY_TIMEOUT));
        assertThat(config.getScoreIfUnknown(), equalTo(FTConfigConstants.SCORE_IF_UNKNOWN));
        assertThat(config.isProfilingOn(), equalTo(false));
        assertThat(config.getMaxFreeMemoryUsage(), equalTo(FTConfigConstants.MAX_FREE_MEMORY_USAGE));
        assertThat(config.getMemoryLimit(), equalTo(null));
        assertThat(config.getParserConfig(), equalTo(FTConfigConstants.DEFAULT_FILE_PARSER_CONFIG));

        assertThat(config.getCanonicalURIsFileName(), is(FTConfigConstants.CANONICAL_URI_FILE_NAME));
        assertThat(config.getSameAsLinkTypes(), is(FTConfigConstants.SAME_AS_LINK_TYPES));
        assertThat(config.getWriteMetadata(), is(FTConfigConstants.WRITE_METADATA));
        assertThat(config.getDataGraphSymbolicName(), is(FTConfigConstants.DEFAULT_DATA_GRAPH_NAME));
        assertThat(config.getMetadataGraphSymbolicName(), is(FTConfigConstants.DEFAULT_METADATA_GRAPH_NAME));
    }

    @Test(expected = InvalidInputException.class)
    public void throwsInvalidInputExceptionWhenInputFileInvalid() throws Exception {
        // Arrange
        String configString = getResourceString("/config/sample-config-invalid.xml");

        // Act
        ConfigReader.parseConfigXml(configString);
    }

    private String getResourceString(String resourcePath) {
        try {
            InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream(resourcePath), Charsets.UTF_8);
            return CharStreams.toString(reader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}