package cz.cuni.mff.xrg.odcs.dpu.fusiontool;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.simpleframework.xml.core.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.xrg.odcs.commons.data.DataUnitException;
import cz.cuni.mff.xrg.odcs.commons.dpu.DPUContext;
import cz.cuni.mff.xrg.odcs.commons.dpu.DPUException;
import cz.cuni.mff.xrg.odcs.commons.dpu.annotation.AsTransformer;
import cz.cuni.mff.xrg.odcs.commons.dpu.annotation.InputDataUnit;
import cz.cuni.mff.xrg.odcs.commons.dpu.annotation.OutputDataUnit;
import cz.cuni.mff.xrg.odcs.commons.module.dpu.ConfigurableBase;
import cz.cuni.mff.xrg.odcs.commons.web.AbstractConfigDialog;
import cz.cuni.mff.xrg.odcs.commons.web.ConfigDialogProvider;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.ConfigContainer;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.ConfigContainerImpl;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.ConfigReader;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.exceptions.FusionToolDpuException;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.exceptions.InvalidInputException;
import cz.cuni.mff.xrg.odcs.rdf.interfaces.RDFDataUnit;

/**
 * Implementation of ODCS-FusionTool as an ODCleanStore DPU.
 * Fuses RDF data from input using ODCS Conflict Resolution.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 * @author Jan Michelfeit
 */
@AsTransformer
public class FusionToolDpu extends ConfigurableBase<FusionToolConfig> implements ConfigDialogProvider<FusionToolConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(FusionToolDpu.class);

    // CHECKSTYLE:OFF
    /**
     * Input data to be processed by data fusion.
     */
    @InputDataUnit(name = "input", optional = false,
            description = "Input data to be processed by data fusion with conflict resolution")
    public RDFDataUnit rdfInput;

    /**
     * owl:sameAs links to be used during conflict resolution.
     */
    @InputDataUnit(name = "sameAs", optional = true, description = "owl:sameAs links to be used during conflict resolution")
    public RDFDataUnit sameAsInput;
    
    /**
     * Metadata used during conflict resolution.
     */
    @InputDataUnit(name = "metadata", optional = true, description = "Metadata used during conflict resolution")
    public RDFDataUnit metadataInput;

    /**
     * Fused output data.
     */
    @OutputDataUnit(name = "output", description = "Fused output data")
    public RDFDataUnit rdfOutput;
    // CHECKSTYLE:ON

    /**
     * Initializes a new instance.
     */
    public FusionToolDpu() {
        super(FusionToolConfig.class);
    }

    @Override
    public AbstractConfigDialog<FusionToolConfig> getConfigurationDialog() {
        return new FusionToolDialog();
    }

    @Override
    public void execute(DPUContext context) throws DPUException, DataUnitException {
        // Read config
        ConfigContainer configContainer = null;
        try {
            configContainer = ConfigReader.parseConfigXml(this.config.getXmlConfig());
            checkValidInput(configContainer);
        } catch (InvalidInputException e) {
            LOG.error("Error in XML configuration:\n  {}", e.getMessage());
            if (e.getCause() instanceof PersistenceException) {
                LOG.error("  " + e.getCause().getMessage());
            }
            throw new DPUException("Error in XML configuration", e);
        }
        
        ((ConfigContainerImpl) configContainer).setProfilingOn(context.isDebugging());
        
        // Start time measurement
        long startTime = System.currentTimeMillis();
        LOG.info("Starting data fusion, this may take a while...");
        try {
            // Execute data fusion
            FusionToolDpuExecutor executor = new FusionToolDpuExecutor(
                    configContainer, 
                    context, 
                    rdfInput,
                    sameAsInput,
                    metadataInput, 
                    rdfOutput);
            executor.runFusionTool();
        } catch (FusionToolDpuException e) {
            LOG.error(e.getMessage());
            if (e.getCause() != null) {
                LOG.error("  " + e.getCause().getMessage());
            }
            throw e;
        }

        LOG.info("Fusion Tool DPU executed in {}", formatRunTime(System.currentTimeMillis() - startTime));
    }

    private static void checkValidInput(ConfigContainer config) throws InvalidInputException {
        if (!ODCSUtils.isValidIRI(config.getResultDataURIPrefix())) {
            throw new InvalidInputException("Result data URI prefix must be a valid URI, '" + config.getResultDataURIPrefix()
                    + "' given");
        }
        for (Map.Entry<String, String> prefixEntry : config.getPrefixes().entrySet()) {
            if (!prefixEntry.getKey().isEmpty() && !ODCSUtils.isValidNamespacePrefix(prefixEntry.getKey())) {
                throw new InvalidInputException("Invalid namespace prefix '" + prefixEntry.getKey() + "'");
            }
            if (!prefixEntry.getValue().isEmpty() && !ODCSUtils.isValidIRI(prefixEntry.getValue())) {
                throw new InvalidInputException("Invalid namespace prefix definition for URI '" + prefixEntry.getValue() + "'");
            }
        }
        // intentionally do not check canonical URI files
    }
    
    private static String formatRunTime(long runTime) {
        final long hourMs = ODCSUtils.MILLISECONDS * ODCSUtils.TIME_UNIT_60 * ODCSUtils.TIME_UNIT_60;
        DateFormat timeFormat = new SimpleDateFormat("mm:ss.SSS");
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return String.format("%d:%s",
                runTime / hourMs,
                timeFormat.format(new Date(runTime)));
    }
}
