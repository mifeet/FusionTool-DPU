/**
 *
 */
package eu.unifiedviews.plugins.transformer.fusiontool.config;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.xml.*;
import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;
import cz.cuni.mff.odcleanstore.fusiontool.util.NamespacePrefixExpander;
import eu.unifiedviews.plugins.transformer.fusiontool.config.xml.ConfigXml;
import eu.unifiedviews.plugins.transformer.fusiontool.config.xml.FileOutputXml;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.InvalidInputException;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;

import java.io.File;
import java.util.*;

/**
 * Reads the XML configuration file and produces instances of configuration in a {@link ConfigContainer} instance.
 * @author Jan Michelfeit
 */
public final class ConfigReader {
    /**
     * Parses the given configuration file and produces returns the contained
     * configuration as an {@link ConfigContainer} instance.
     * @param configString configuration XMl file
     * @return parsed configuration
     * @throws InvalidInputException parsing error
     */
    public static ConfigContainer parseConfigXml(String configString) throws InvalidInputException {
        ConfigReader instance = new ConfigReader();
        return instance.parseConfigXmlImpl(configString);
    }

    private ConfigContainer parseConfigXmlImpl(String configString) throws InvalidInputException {
        ConfigContainerImpl config = new ConfigContainerImpl();
        if (ODCSUtils.isNullOrEmpty(configString.trim())) {
            return config;
        }

        Serializer serializer = new Persister();
        ConfigXml configXml;
        try {
            configXml = serializer.read(ConfigXml.class, configString);
        } catch (Exception e) {
            throw new InvalidInputException("Error parsing configuration file", e);
        }

        // Prefixes
        Map<String, String> prefixes;
        if (configXml.getPrefixes() != null) {
            prefixes = extractPrefixes(configXml.getPrefixes());
        } else {
            prefixes = new HashMap<>();
        }
        config.setPrefixes(Collections.unmodifiableMap(prefixes));
        NamespacePrefixExpander prefixExpander = new NamespacePrefixExpander(config.getPrefixes());

        // Data processing settings
        if (configXml.getDataProcessing() != null) {
            List<ParamXml> params = configXml.getDataProcessing().getParams();
            if (params != null) {
                extractDataProcessingParams(params, config, prefixExpander);
            }
        }

        // Conflict resolution settings
        if (configXml.getConflictResolution() != null) {
            ConflictResolutionXml crXml = configXml.getConflictResolution();
            if (crXml.getDefaultResolutionStrategy() != null) {
                config.setDefaultResolutionStrategy(extractResolutionStrategy(
                        crXml.getDefaultResolutionStrategy(), prefixExpander));
            }
            config.setPropertyResolutionStrategies(extractPropertyResolutionStrategies(
                    crXml.getPropertyResolutionStrategies(), prefixExpander));
        }

        // File outputs
        List<FileOutput> outputs = new LinkedList<>();
        if (configXml.getFileOutputs() != null) {
            for (FileOutputXml fileOutputXml : configXml.getFileOutputs().getFileOutputs()) {
                outputs.add(extractFileOutput(fileOutputXml));
            }
            if (configXml.getFileOutputs().getMaxResolvedQuads() != null) {
                config.setMaxOutputTriples(configXml.getFileOutputs().getMaxResolvedQuads());
            }
        }
        config.setFileOutputs(outputs);
        return config;
    }

    private Map<URI, ResolutionStrategy> extractPropertyResolutionStrategies(
            List<PropertyResolutionStrategyXml> propertyResolutionStrategies,
            NamespacePrefixExpander prefixExpander)
            throws InvalidInputException {
        Map<URI, ResolutionStrategy> result = new HashMap<>(propertyResolutionStrategies.size());
        for (PropertyResolutionStrategyXml strategyXml : propertyResolutionStrategies) {
            ResolutionStrategy strategy = extractResolutionStrategy(strategyXml, prefixExpander);
            for (PropertyXml propertyXml : strategyXml.getProperties()) {
                URI uri = convertToUriWithExpansion(prefixExpander, propertyXml.getId());
                result.put(uri, strategy);
            }
        }
        return result;
    }

    private ResolutionStrategy extractResolutionStrategy(ResolutionStrategyXml strategyXml, NamespacePrefixExpander prefixExpander) throws InvalidInputException {
        ResolutionStrategyImpl strategy = new ResolutionStrategyImpl();
        strategy.setResolutionFunctionName(strategyXml.getResolutionFunctionName());
        strategy.setCardinality(strategyXml.getCardinality());
        strategy.setAggregationErrorStrategy(strategyXml.getAggregationErrorStrategy());
        if (strategyXml.getDependsOn() != null) {
            strategy.setDependsOn(convertToUriWithExpansion(prefixExpander, strategyXml.getDependsOn()));
        }
        if (strategyXml.getParams() != null) {
            strategy.setParams(extractAllParams(strategyXml.getParams()));
        }
        return strategy;
    }


    private FileOutput extractFileOutput(FileOutputXml fileOutputXml) throws InvalidInputException {
        File path = new File(fileOutputXml.getPath());
        if (path.getAbsoluteFile().getParentFile() == null) {
            throw new InvalidInputException("Path '" + fileOutputXml.getPath() + "' is not a valid file path.");
        }

        EnumSerializationFormat format = EnumSerializationFormat.parseFormat(fileOutputXml.getFormat());
        if (format == null) {
            throw new InvalidInputException("Unknown file output format '" + fileOutputXml.getFormat() + "'");
        }

        FileOutputImpl output = new FileOutputImpl(path, format);

        String metadataContextString = fileOutputXml.getMetadataContext();
        if (!ODCSUtils.isNullOrEmpty(metadataContextString)) {
            URI context = convertToURI(metadataContextString, "metadataContext is not a valid URI");
            output.setMetadataContext(context);
        }

        String dataContextString = fileOutputXml.getDataContext();
        if (!ODCSUtils.isNullOrEmpty(dataContextString)) {
            URI context = convertToURI(dataContextString, "dataContext is not a valid URI");
            output.setDataContext(context);
        }

        return output;
    }

    private Map<String, String> extractAllParams(List<ParamXml> params) {
        Map<String, String> result = new HashMap<>(params.size());
        for (ParamXml param : params) {
            result.put(param.getName(), param.getValue());
        }
        return result;
    }

    private Map<String, String> extractPrefixes(List<PrefixXml> prefixes) {
        Map<String, String> prefixMap = new HashMap<>();
        for (PrefixXml prefixXml : prefixes) {
            prefixMap.put(prefixXml.getId(), prefixXml.getNamespace());
        }
        return prefixMap;
    }

    private void extractDataProcessingParams(
            List<ParamXml> params,
            ConfigContainerImpl config,
            NamespacePrefixExpander prefixExpander) throws InvalidInputException {
        for (ParamXml param : params) {
            if (ConfigParameters.PROCESSING_ONLY_RESOURCES_WITH_CLASS.equalsIgnoreCase(param.getName())) {
                if (!ODCSUtils.isNullOrEmpty(param.getValue())) {
                    URI classUri = convertToUriWithExpansion(prefixExpander, param.getValue());
                    config.setRequiredClassOfProcessedResources(classUri);
                }
            } else {
                throw new InvalidInputException("Unknown parameter " + param.getName()
                        + " used in data processing parameters");
            }
        }
    }

    private URI convertToURI(String str, String errorMessage) throws InvalidInputException {
        try {
            return ValueFactoryImpl.getInstance().createURI(str);
        } catch (IllegalArgumentException e) {
            throw new InvalidInputException(errorMessage, e);
        }
    }

    private URI convertToUriWithExpansion(NamespacePrefixExpander prefixExpander, String value) throws InvalidInputException {
        try {
            return prefixExpander.convertToUriWithExpansion(value);
        } catch (cz.cuni.mff.odcleanstore.fusiontool.exceptions.InvalidInputException e) {
            throw new InvalidInputException(e.getMessage(), e);
        }
    }

    private long convertToLong(String str, String errorMessage) throws InvalidInputException {
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            throw new InvalidInputException(errorMessage, e);
        }
    }

    private ConfigReader() {
    }
}
