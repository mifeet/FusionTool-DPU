package eu.unifiedviews.plugins.transformer.fusiontool;

import com.google.common.collect.ImmutableList;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.FusionRunner;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.EnumFusionCounters;
import cz.cuni.mff.odcleanstore.fusiontool.util.LDFusionToolUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.ProfilingTimeCounter;
import eu.unifiedviews.dataunit.DataUnit;
import eu.unifiedviews.dataunit.rdf.RDFDataUnit;
import eu.unifiedviews.dataunit.rdf.WritableRDFDataUnit;
import eu.unifiedviews.dpu.DPU;
import eu.unifiedviews.dpu.DPUContext;
import eu.unifiedviews.dpu.DPUException;
import eu.unifiedviews.helpers.dpu.config.AbstractConfigDialog;
import eu.unifiedviews.helpers.dpu.config.ConfigDialogProvider;
import eu.unifiedviews.helpers.dpu.config.ConfigurableBase;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainer;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigContainerImpl;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigReader;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.FusionToolDpuErrorCodes;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.FusionToolDpuException;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.InvalidInputException;
import org.simpleframework.xml.core.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
        setLogLevel(context.isDebugging());

        // Read config
        ConfigContainer configContainer;
        try {
            configContainer = ConfigReader.parseConfigXml(this.config.getXmlConfig());
            ((ConfigContainerImpl) configContainer).setProfilingOn(context.isDebugging());
            checkValidInput(configContainer);
        } catch (InvalidInputException e) {
            LOG.error("Error in XML configuration:\n  {}", e.getMessage());
            if (e.getCause() instanceof PersistenceException) {
                LOG.error("  " + e.getCause().getMessage());
            }
            throw new DPUException("Error in XML configuration", e);
        }

        // Start time measurement
        long startTime = System.currentTimeMillis();
        LOG.info("Starting data fusion, this may take a while...");

        try {
            List<RDFDataUnit> rdfInputs = ImmutableList.of(rdfInput, rdfInput2);

            // Execute data fusion
            FusionToolDpuComponentFactory componentFactory = new FusionToolDpuComponentFactory(
                    configContainer,
                    context,
                    rdfInputs,
                    sameAsInput,
                    metadataInput,
                    rdfOutput);
            FusionRunner runner = new FusionRunner(componentFactory);
            runner.setProfilingOn(configContainer.isProfilingOn());
            runner.runFusionTool();

            if (configContainer.isProfilingOn()) {
                printProfilingInformation(componentFactory, runner);
            }

        } catch (ConflictResolutionException | IOException | LDFusionToolException e) {
            handleException(e);
        }

        LOG.info("Fusion Tool DPU executed in {}", LDFusionToolUtils.formatTime(System.currentTimeMillis() - startTime));
    }

    private static void checkValidInput(ConfigContainer config) throws InvalidInputException {
        for (Map.Entry<String, String> prefixEntry : config.getPrefixes().entrySet()) {
            if (!prefixEntry.getKey().isEmpty() && !ODCSUtils.isValidNamespacePrefix(prefixEntry.getKey())) {
                throw new InvalidInputException("Invalid namespace prefix '" + prefixEntry.getKey() + "'");
            }
            if (!prefixEntry.getValue().isEmpty() && !ODCSUtils.isValidIRI(prefixEntry.getValue())) {
                throw new InvalidInputException("Invalid namespace prefix definition for URI '" + prefixEntry.getValue() + "'");
            }
        }
    }

    private static void setLogLevel(boolean isDebugging) {
        //Level level = isDebugging ? Level.DEBUG : Level.INFO;
        //ch.qos.logback.classic.Logger logger1 = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FusionToolDpu.class.getPackage().getName());
        //logger1.setLevel(level);
        //ch.qos.logback.classic.Logger logger2 = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ResourceDescriptionConflictResolver.class.getPackage().getName());
        //logger2.setLevel(level);
    }

    private static void handleException(Exception e) throws DPUException {
        LOG.error(e.getMessage());
        if (e.getCause() != null) {
            LOG.error("  " + e.getCause().getMessage());
        }
        throw new FusionToolDpuException(FusionToolDpuErrorCodes.FUSION_TOOL_EXECUTION_ERROR, e);
    }

    private void printProfilingInformation(FusionToolDpuComponentFactory componentFactory, FusionRunner runner) {
        ProfilingTimeCounter<EnumFusionCounters> timeProfiler = runner.getTimeProfiler();
        timeProfiler.addProfilingTimeCounter(componentFactory.getExecutorTimeProfiler());
        LOG.info("-- Profiling information --------");
        LOG.info("Initialization time:              " + timeProfiler.formatCounter(EnumFusionCounters.INITIALIZATION));
        LOG.info("Reading metadata & sameAs links:  " + timeProfiler.formatCounter(EnumFusionCounters.META_INITIALIZATION));
        LOG.info("Data preparation time:            " + timeProfiler.formatCounter(EnumFusionCounters.DATA_INITIALIZATION));
        LOG.info("Triple loading time:              " + timeProfiler.formatCounter(EnumFusionCounters.QUAD_LOADING));
        LOG.info("Input filtering time:             " + timeProfiler.formatCounter(EnumFusionCounters.INPUT_FILTERING));
        LOG.info("Buffering time:                   " + timeProfiler.formatCounter(EnumFusionCounters.BUFFERING));
        LOG.info("Conflict resolution time:         " + timeProfiler.formatCounter(EnumFusionCounters.CONFLICT_RESOLUTION));
        LOG.info("Output writing time:              " + timeProfiler.formatCounter(EnumFusionCounters.OUTPUT_WRITING));
    }

}
