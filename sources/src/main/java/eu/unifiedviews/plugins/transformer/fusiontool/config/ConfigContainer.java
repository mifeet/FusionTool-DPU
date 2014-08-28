package eu.unifiedviews.plugins.transformer.fusiontool.config;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConflictResolution;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigData;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigProcessing;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigQuality;

import java.util.List;

/**
 * Encapsulation of ODCS-FusionTool configuration.
 * @author Jan Michelfeit
 */
public interface ConfigContainer extends ConfigData, ConfigProcessing, ConfigQuality, ConfigConflictResolution {
    /**
     * Name of file where resolved canonical URIs are read from and written to. 
     * Null means that canonical URIs will not be written anywhere.
     * @return name of file with canonical URIs or null
     */
    String getCanonicalURIsFileName();
    
    /**
     * List of result data file outputs.
     * @return list of result data file outputs
     */
    List<FileOutput> getFileOutputs();
    
    /**
     * SPARQL query returning URI resources which are initially loaded and processed.
     * The query must be a SELECT query binding a single variable in the result set.
     * If given, triples having matching resources and triples reachable from them are processed. All data
     * from matching input graphs are processed otherwise.
     * @return SPARQL query or null  
     */
    String getSeedResourceSparqlQuery();
}