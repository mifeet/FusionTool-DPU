package eu.unifiedviews.plugins.transformer.fusiontool;

import eu.unifiedviews.plugins.transformer.fusiontool.config.ConfigReader;
import eu.unifiedviews.plugins.transformer.fusiontool.exceptions.InvalidInputException;

import java.io.Serializable;

/**
 * Put your DPU's configuration here.
 * 
 * You can optionally implement {@link #isValid()} to provide possibility
 * to validate the configuration.
 * 
 * <b>This class must have default (parameter less) constructor!</b>
 * @author Jan Michelfeit
 */
public class FusionToolConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String xmlConfig;

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

    public boolean isValid() {
        try {
            ConfigReader.parseConfigXml(this.xmlConfig);
        } catch (InvalidInputException e) {
            return false;
        }
        return true;
    }

}
