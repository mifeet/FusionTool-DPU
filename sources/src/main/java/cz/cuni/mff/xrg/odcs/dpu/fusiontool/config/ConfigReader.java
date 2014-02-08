/**
 * 
 */
package cz.cuni.mff.xrg.odcs.dpu.fusiontool.config;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml.ConfigXml;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml.ConflictResolutionXml;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml.FileOutputXml;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml.ParamXml;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml.PrefixXml;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml.PropertyResolutionStrategyXml;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml.PropertyXml;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.xml.ResolutionStrategyXml;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.exceptions.InvalidInputException;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.io.EnumSerializationFormat;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.io.FileNameSanitizer;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.util.NamespacePrefixExpander;

/**
 * Reads the XML configuration file and produces instances of configuration in a {@link ConfigContainer} instance.
 * @author Jan Michelfeit
 */
public final class ConfigReader {
    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();
    
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
            prefixes = new HashMap<String, String>();
        }
        config.setPrefixes(Collections.unmodifiableMap(prefixes));

        // Data processing settings
        if (configXml.getDataProcessing() != null) {
            config.setSeedResourceSparqlQuery(configXml.getDataProcessing().getSeedResourceSparqlQuery());
            List<ParamXml> params = configXml.getDataProcessing().getParams();
            if (params != null) {
                extractDataProcessingParams(params, config);
            }
        }
        
        // Conflict resolution settings
        NamespacePrefixExpander prefixExpander = new NamespacePrefixExpander(config.getPrefixes());
        if (configXml.getConflictResolution() != null) {
            ConflictResolutionXml crXml = configXml.getConflictResolution();
            if (crXml.getDefaultResolutionStrategy() != null) {
                config.setDefaultResolutionStrategy(extractResolutionStrategy(crXml.getDefaultResolutionStrategy()));
            } 
            config.setPropertyResolutionStrategies(extractPropertyResolutionStrategies(
                    crXml.getPropertyResolutionStrategies(),
                    prefixExpander));
        }

        // File outputs
        List<FileOutput> outputs = new LinkedList<FileOutput>();
        if (configXml.getFileOutputs() != null) {
            for (FileOutputXml fileOutputXml : configXml.getFileOutputs().getFileOutputs()) {
                outputs.add(extractFileOutput(fileOutputXml));
            }
            if (configXml.getFileOutputs().getMaxResolvedQuads() != null) {
                config.setFileOutputMaxResolvedQuads(configXml.getFileOutputs().getMaxResolvedQuads());
            }
        }
        config.setFileOutputs(outputs);
        return config;
    }

    private Map<URI, ResolutionStrategy> extractPropertyResolutionStrategies(
            List<PropertyResolutionStrategyXml> propertyResolutionStrategies,
            NamespacePrefixExpander prefixExpander)
            throws InvalidInputException {
        Map<URI, ResolutionStrategy> result = new HashMap<URI, ResolutionStrategy>(propertyResolutionStrategies.size());
        for (PropertyResolutionStrategyXml strategyXml : propertyResolutionStrategies) {
            ResolutionStrategy strategy = extractResolutionStrategy(strategyXml);
            for (PropertyXml propertyXml : strategyXml.getProperties()) {
                URI uri = VALUE_FACTORY.createURI(prefixExpander.expandPrefix(propertyXml.getId()));
                result.put(uri, strategy);
            }
        }
        return result;
    }

    private ResolutionStrategy extractResolutionStrategy(ResolutionStrategyXml strategyXml) {
        ResolutionStrategyImpl strategy = new ResolutionStrategyImpl();
        strategy.setResolutionFunctionName(strategyXml.getResolutionFunctionName());
        strategy.setCardinality(strategyXml.getCardinality());
        strategy.setAggregationErrorStrategy(strategyXml.getAggregationErrorStrategy());
        if (strategyXml.getParams() != null) {
            strategy.setParams(extractAllParams(strategyXml.getParams()));
        }
        return strategy;
    }
    

    private FileOutput extractFileOutput(FileOutputXml fileOutputXml) throws InvalidInputException {
        File path = new File(fileOutputXml.getPath());
        if (path.getAbsoluteFile() == null || path.getAbsoluteFile().getParentFile() == null) {
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
    
    private URI convertToURI(String str, String errorMessage) throws InvalidInputException {
        try {
            return ValueFactoryImpl.getInstance().createURI(str);
        } catch (IllegalArgumentException e) {
            throw new InvalidInputException(errorMessage, e);
        }
    }

    private Map<String, String> extractAllParams(List<ParamXml> params) {
        Map<String, String> result = new HashMap<String, String>(params.size());
        for (ParamXml param : params) {
            result.put(param.getName(), param.getValue());
        }
        return result;
    }

    private Map<String, String> extractPrefixes(List<PrefixXml> prefixes) {
        Map<String, String> prefixMap = new HashMap<String, String>();
        for (PrefixXml prefixXml : prefixes) {
            prefixMap.put(prefixXml.getId(), prefixXml.getNamespace());
        }
        return prefixMap;
    }

    private void extractDataProcessingParams(List<ParamXml> params, ConfigContainerImpl config) throws InvalidInputException {
        for (ParamXml param : params) {
            if (param.getValue() == null) {
                continue;
            } else if ("enableFileCache".equalsIgnoreCase(param.getName())) {
                config.setEnableFileCache(Boolean.parseBoolean(param.getValue()));
            } else if ("canonicalUrisFile".equalsIgnoreCase(param.getName())) {
                String fileName = param.getValue();
                if (ODCSUtils.isNullOrEmpty(fileName)) {
                    continue;
                } else if (FileNameSanitizer.containsPath(fileName)) {
                    throw new InvalidInputException("Parameter " + param.getName()
                            + " must be a simple file name and cannot contain path");
                } else if (!FileNameSanitizer.isFileNameValid(fileName)) {
                    throw new InvalidInputException("Parameter " + param.getName()
                            + " contains illegal characters for a file name");
                } else {
                    config.setCanonicalURIsFileName(fileName);
                }
            } else {
                throw new InvalidInputException("Unknown parameter " + param.getName()
                        + " used in conflict resolution parameters");
            }
        }
    }

    private ConfigReader() {
    }
}
