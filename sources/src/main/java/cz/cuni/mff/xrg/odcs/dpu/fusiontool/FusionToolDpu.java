package cz.cuni.mff.xrg.odcs.dpu.fusiontool;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolverImpl;
import cz.cuni.mff.xrg.odcs.commons.data.DataUnitException;
import cz.cuni.mff.xrg.odcs.commons.dpu.DPUContext;
import cz.cuni.mff.xrg.odcs.commons.dpu.DPUException;
import cz.cuni.mff.xrg.odcs.commons.dpu.annotation.AsTransformer;
import cz.cuni.mff.xrg.odcs.commons.dpu.annotation.InputDataUnit;
import cz.cuni.mff.xrg.odcs.commons.dpu.annotation.OutputDataUnit;
import cz.cuni.mff.xrg.odcs.commons.module.dpu.ConfigurableBase;
import cz.cuni.mff.xrg.odcs.commons.module.file.FileManager;
import cz.cuni.mff.xrg.odcs.commons.web.AbstractConfigDialog;
import cz.cuni.mff.xrg.odcs.commons.web.ConfigDialogProvider;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.ConfigContainer;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.ConfigReader;
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
    public RDFDataUnit sameAs;

    /**
     * Fused output data.
     */
    @OutputDataUnit(name = "output", description = "Fused output data")
    public RDFDataUnit rdfOutput;

    /**
     * Quality & provenance metadata about the fused output data.
     */
    @OutputDataUnit(name = "metadata", description = "Quality & provenance metadata about the fused output data")
    public RDFDataUnit metadataOutput;
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
    public void execute(DPUContext context) throws DPUException,
            DataUnitException {

        ConfigContainer configContainer = null;
        try {
            configContainer = ConfigReader.parseConfigXml(this.config.getXmlConfig());
//            checkValidInput(config);
//            setupDefaultRestrictions(config);
        } catch (InvalidInputException e) {
//            System.err.println("Error in config file:");
//            System.err.println("  " + e.getMessage());
//            if (e.getCause() instanceof PersistenceException) {
//                System.err.println("  " + e.getCause().getMessage());
//            }
//            e.printStackTrace();
            return;
        }

//        long startTime = System.currentTimeMillis();
//        //System.out.println("Starting conflict resolution, this may take a while... \n");
//
//        try {
//            ODCSFusionToolExecutor odcsFusionToolExecutor = new ODCSFusionToolExecutor();
//            odcsFusionToolExecutor.runFusionTool(config);
//        } catch (ODCSFusionToolException e) {
//            System.err.println("Error:");
//            System.err.println("  " + e.getMessage());
//            if (e.getCause() != null) {
//                System.err.println("  " + e.getCause().getMessage());
//            }
//            return;
//        } catch (ConflictResolutionException e) {
//            System.err.println("Conflict resolution error:");
//            System.err.println("  " + e.getMessage());
//            return;
//        } catch (IOException e) {
//            System.err.println("Error when writing results:");
//            System.err.println("  " + e.getMessage());
//            return;
//        }
//
//        System.out.println("----------------------------");
//
//        long runTime = System.currentTimeMillis() - startTime;
//        final long hourMs = ODCSUtils.MILLISECONDS * ODCSUtils.TIME_UNIT_60 * ODCSUtils.TIME_UNIT_60;
//        DateFormat timeFormat = new SimpleDateFormat("mm:ss.SSS");
//        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
//        System.out.printf("ODCS-FusionTool executed in %d:%s \n",
//                runTime / hourMs,
//                timeFormat.format(new Date(runTime)));

        FileManager fileManager = new FileManager(context);
        File testFile = fileManager.getGlobal().file("test-dpu.txt");
        PrintWriter writer;
        try {
            writer = new PrintWriter(new FileWriter(testFile));
            writer.write("Current time: " + (new Date()).toString() + "\n");
            writer.write("Config: " + configContainer.getEnableFileCache() + "\n");

            ConflictResolver resolver = new ConflictResolverImpl();
            Collection<ResolvedStatement> resolvedTriples;
            try {
                resolvedTriples = resolver.resolveConflicts(rdfInput.getTriples());
            } catch (Exception e) {
                e.printStackTrace(writer);
                LOG.error("ERROR", e);
            } finally {
                writer.close();
            }
        } catch (IOException e) {
            throw new DPUException(e);
        }
    }

}
