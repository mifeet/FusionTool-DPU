package cz.cuni.mff.xrg.odcs.dpu.fusiontool;

import com.vaadin.data.Property;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.TextArea;

import cz.cuni.mff.xrg.odcs.commons.configuration.ConfigException;
import cz.cuni.mff.xrg.odcs.commons.module.dialog.BaseConfigDialog;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.config.ConfigReader;
import cz.cuni.mff.xrg.odcs.dpu.fusiontool.exceptions.InvalidInputException;

/**
 * DPU's configuration dialog. User can use this dialog to configure DPU
 * configuration.
 * @author Jan Michelfeit
 */
public class FusionToolDialog extends
        BaseConfigDialog<FusionToolConfig> {

    private static final long serialVersionUID = 1L;

    private GridLayout mainLayout;

    private TextArea configTextArea;

    private Label labelUpQuer;

    /**
     * Initializes a new instance of the class.
     */
    public FusionToolDialog() {
        super(FusionToolConfig.class);
        buildMainLayout();
        setCompositionRoot(mainLayout);
    }

    @Override
    public void setConfiguration(FusionToolConfig conf)
            throws ConfigException {
        configTextArea.setValue(conf.getXmlConfig());
    }

    @Override
    public FusionToolConfig getConfiguration() throws ConfigException {
        if (!configTextArea.isValid()) {
            throw new ConfigException("Invalid configuration");
        } else {
            FusionToolConfig conf = new FusionToolConfig(configTextArea.getValue().trim());
            return conf;
        }
    }

    @Override
    public String getToolTip() {
        return super.getToolTip();
    }

    @Override
    public String getDescription() {
        return super.getDescription();
    }

    /**
     * Builds main layout with all dialog components.
     * 
     * @return mainLayout GridLayout with all components of configuration
     *         dialog.
     */
    private GridLayout buildMainLayout() {
        // common part: create layout
        mainLayout = new GridLayout(2, 1);
        mainLayout.setImmediate(false);
        mainLayout.setWidth("100%");
        mainLayout.setHeight("100%");
        mainLayout.setMargin(false);

        // top-level component properties
        setWidth("100%");
        setHeight("100%");

        // labelUpQuer
        labelUpQuer = new Label();
        labelUpQuer.setImmediate(false);
        labelUpQuer.setWidth("68px");
        labelUpQuer.setHeight("-1px");
        labelUpQuer.setValue("Configuration");
        mainLayout.addComponent(labelUpQuer, 0, 0);

        // SPARQL Update Query textArea
        configTextArea = new TextArea();

        configTextArea
                .addValueChangeListener(new Property.ValueChangeListener() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void valueChange(Property.ValueChangeEvent event) {
                        // final String query = configTextArea.getValue().trim();
                        // if not valid
                        // validationErrorMessage = "...";
                    }
                });

        configTextArea.addValidator(new com.vaadin.data.Validator() {
            private static final long serialVersionUID = 1L;

            @Override
            public void validate(Object value) throws InvalidValueException {
                try {
                    ConfigReader.parseConfigXml(value.toString());
                } catch (InvalidInputException e) {
                    throw new InvalidValueException("Invalid XML configuration");
                }
            }
        });

        // configTextArea.setNullRepresentation("");
        configTextArea.setImmediate(true);
        configTextArea.setWidth("100%");
        configTextArea.setHeight("211px");
        configTextArea.setInputPrompt("<?xml version=\"1.0\"?>\n<config>\n</config>");

        mainLayout.addComponent(configTextArea, 1, 0);
        // CHECKSTYLE:OFF
        mainLayout.setColumnExpandRatio(0, 0.00001f);
        mainLayout.setColumnExpandRatio(1, 0.99999f);
        // CHECKSTYLE:ON

        return mainLayout;
    }
}
