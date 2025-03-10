package easytest.plugin.mutation;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

public class MutateTransformConfig extends PluginConfig {

  public static final String PLUGIN_NAME = "MutateTransform";
  public static final String PLUGIN_DESCRIPTION = "Mutates records based on a probability distribution to facilitate testing.";

  private static final String FIELD_SAMPLE_SIZE = "sample-size";
  private static final String FIELD_WEIGHT_VALUES = "weight-values";
  private static final String FIELD_MUTATIONS_DROPDOWN = "mutations-dropdown";

  @Name(FIELD_SAMPLE_SIZE)
  @Description("Specifies the number of samples to mutate.")
  public final int sampleSize;

  @Name(FIELD_WEIGHT_VALUES)
  @Description("A set of mutation probabilities associated with the fields.")
  @Nullable
  @Macro
  public final String weights;

  @Name(FIELD_MUTATIONS_DROPDOWN)
  @Description("Specifies the type of mutations to apply to the fields.")
  public final String mutations;

  public MutateTransformConfig(String schema, int sampleSize, String weights, String mutations) {
    this.sampleSize = sampleSize;
    this.weights = weights;
    this.mutations = mutations;
  }

  public void validate() throws IllegalArgumentException {
    // This method should be used to validate that the configuration is valid.
    if (this.mutations == null || this.mutations.isEmpty()) {
      throw new IllegalArgumentException("At least one mutation must be specified.");
    }
    // You can use the containsMacro() function to determine if you can validate at
    // deploy time or runtime.
    // If your plugin depends on fields from the input schema being present or the
    // right type, use inputSchema
  }
}
