package eu.unifiedviews.plugins.transformer.fusiontool;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import org.junit.Test;

import java.io.InputStreamReader;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class FusionToolConfigTest {
    @Test
    public void setsConfigString() throws Exception {
        FusionToolConfig fusionToolConfig = new FusionToolConfig("<Config />");
        assertThat(fusionToolConfig.getXmlConfig(), is("<Config />"));
    }

    @Test
    public void isValidReturnsTrueForValidConfig() throws Exception {
        String configString = getResourceString("/config/sample-config-minimal.xml");
        FusionToolConfig fusionToolConfig = new FusionToolConfig(configString);
        assertThat(fusionToolConfig.isValid(), is(true));
    }

    @Test
    public void isValidReturnsFalseForInvalidConfig() throws Exception {
        String configString = getResourceString("/config/sample-config-invalid.xml");
        FusionToolConfig fusionToolConfig = new FusionToolConfig(configString);
        assertThat(fusionToolConfig.isValid(), is(false));
    }

    private String getResourceString(String resourcePath) {
        try {
            InputStreamReader reader = new InputStreamReader(getClass().getResourceAsStream(resourcePath), Charsets.UTF_8);
            return CharStreams.toString(reader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}