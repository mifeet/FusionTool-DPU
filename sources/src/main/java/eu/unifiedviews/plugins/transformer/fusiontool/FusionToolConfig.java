package eu.unifiedviews.plugins.transformer.fusiontool;

import cz.cuni.mff.xrg.odcs.commons.module.config.DPUConfigObjectBase;
import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigReader;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.InvalidInputException;

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

    /**
     * Initializes a new instance with the given XML configuration.
     * @param xmlConfig XML configuration
     */
    public FusionToolConfig(String xmlConfig) {
        this.xmlConfig = xmlConfig;
    }

    /**
     * Returns the XML configuration for DPU.
     * @return XML configuration
     */
    public String getXmlConfig() {
        return xmlConfig;
    }

    @Override
    public boolean isValid() {
        try {
            ConfigReader.parseConfigXml(this.xmlConfig);
        } catch (InvalidInputException e) {
            return false;
        }
        return true;
    }

}
