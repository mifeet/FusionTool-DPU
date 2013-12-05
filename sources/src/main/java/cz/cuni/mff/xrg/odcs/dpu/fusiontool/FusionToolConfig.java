package cz.cuni.mff.xrg.odcs.dpu.fusiontool;

import cz.cuni.mff.xrg.odcs.commons.module.config.DPUConfigObjectBase;

/**
 * Put your DPU's configuration here.
 * 
 * You can optionally implement {@link #isValid()} to provide possibility
 * to validate the configuration.
 * 
 * <b>This class must have default (parameter less) constructor!</b>
 * @author Jan Michelfeit
 */
public class FusionToolConfig extends DPUConfigObjectBase {
    private static final long serialVersionUID = 1L;

    private String xmlConfig;

    /**
     * Initializes a new instance with empty configuration.
     */
    public FusionToolConfig() {
        xmlConfig = "";
    }

    public FusionToolConfig(String xmlConfig) {
        this.xmlConfig = xmlConfig;
    }

    public String getXmlConfig() {
        return xmlConfig;
    }

    @Override
    public boolean isValid() {
        return super.isValid();
    }

}
