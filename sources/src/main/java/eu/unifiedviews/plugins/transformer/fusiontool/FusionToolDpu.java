package eu.unifiedviews.plugins.transformer.fusiontool;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dpu.config.AbstractConfigDialog;
import eu.unifiedviews.helpers.dpu.config.ConfigDialogProvider;
import eu.unifiedviews.helpers.dpu.config.ConfigurableBase;
import org.simpleframework.xml.core.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainer;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainerImpl;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigReader;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.FusionToolDpuException;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.InvalidInputException;

/**
 * Implementation of ODCS-FusionTool as an ODCleanStore DPU.
 * Fuses RDF data from input using ODCS Conflict Resolution.
 * Conflict resolution includes resolution of owl:sameAs link, resolution of instance-level conflicts.
 * @author Jan Michelfeit
 */
@DPU.AsTransformer
public class FusionToolDpu extends ConfigurableBase<FusionToolConfig> implements ConfigDialogProvider<FusionToolConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(FusionToolDpu.class);

    // CHECKSTYLE:OFF
    /**
     * Input data to be processed by data fusion.
     */
    @DataUnit.AsInput(name = "input", optional = false,
            description = "Input data to be processed by data fusion (required)")
    public RDFDataUnit rdfInput;
    
    /**
     * Second source of input data to be processed by data fusion.
     */
    @DataUnit.AsInput(name = "input2", optional = true,
            description = "Second source of input data to be processed by data fusion (optional)")
    public RDFDataUnit rdfInput2;
    
    /**
     * owl:sameAs links to be used during conflict resolution.
     */
    @DataUnit.AsInput(name = "sameAs", optional = true,
        description = "owl:sameAs links to be used during conflict resolution (optional)")
    public RDFDataUnit sameAsInput;
    
    /**
     * Metadata used during conflict resolution.
     */
    @DataUnit.AsInput(name = "metadata", optional = true,
        description = "Metadata used during conflict resolution (optional)")
    public RDFDataUnit metadataInput;

    /**
     * Fused output data.
     */
    @DataUnit.AsOutput(name = "output", optional = false,
            description = "Fused output data")
    public WritableRDFDataUnit rdfOutput;
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
    public void execute(DPUContext context) throws DPUException {
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
            List<RDFDataUnit> rdfInputs = new ArrayList<RDFDataUnit>();
            rdfInputs.add(rdfInput);
            rdfInputs.add(rdfInput2);
            
            // Execute data fusion
            FusionToolDpuExecutor executor = new FusionToolDpuExecutor(
                    configContainer, 
                    context, 
                    rdfInputs,
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
