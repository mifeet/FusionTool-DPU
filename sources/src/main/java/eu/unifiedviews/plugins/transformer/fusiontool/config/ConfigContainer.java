package eu.unifiedviews.plugins.transformer.fusiontool.config;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConflictResolution;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigData;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigProcessing;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigQuality;

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
     * True iff the output should contain metadata (quality and source graphs of resolved triples).
     * Note that when metadata are enabled, each resolved triple will be placed to its own named graph.
     * If metadata are disabled, all resolved triples will be placed in the default graph of output data unit.
     * @return true if the output should contain metadata, false if no metadata are written to the output.
     */
    boolean getWriteMetadata();

    /**
     * Symbolic name of the data graph where output triples are written.
     * @return symbolic name of the output data graph
     * @see eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit#addNewDataGraph(String)
     */
    String getDataGraphSymbolicName();

    /**
     * Symbolic name of the graph where output metadata are written.
     * @return symbolic name of the output metadata graph
     * @see eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit#addNewDataGraph(String)
     */
    String getMetadataGraphSymbolicName();
}