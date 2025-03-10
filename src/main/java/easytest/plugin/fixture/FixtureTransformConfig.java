package easytest.plugin.fixture;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

public class FixtureTransformConfig extends PluginConfig {

  public static final String PLUGIN_NAME = "FixtureTransform";
  public static final String PLUGIN_DESCRIPTION = "A fixture can be used to set up context for the assertions in the pipeline.";

  private static final String FIELD_NAME = "name";
  private static final String FIELD_FIXTURE_VALUES = "fixture-values";
  private static final String FIELD_ASSERTION_CONTROL = "assertion-control";

  @Name(FIELD_NAME)
  @Description("Specifies the name for this fixture. This name can be used in assertions to access the key-value pairs in this fixture.")
  @Macro
  public final String name;

  @Name(FIELD_FIXTURE_VALUES)
  @Description("A set of shared key-value pairs for this fixture. These will be available to use in assertions following this fixture.")
  @Nullable
  @Macro
  public final String fixtureValues;

  @Name(FIELD_ASSERTION_CONTROL)
  @Description("Enable or disable assertions following this fixture in the pipeline.")
  @Nullable
  public final String assertionControl;

  public FixtureTransformConfig(String name, String fixtureValues, String assertionControl) {
    this.name = name;
    this.fixtureValues = fixtureValues;
    this.assertionControl = assertionControl;
  }

  public void validate() throws IllegalArgumentException {
    // This method should be used to validate that the configuration is valid.
    if (this.name == null || this.name.isEmpty()) {
      throw new IllegalArgumentException("Name is a required field.");
    }
    // You can use the containsMacro() function to determine if you can validate at
    // deploy time or runtime.
    // If your plugin depends on fields from the input schema being present or the
    // right type, use inputSchema
  }
}
