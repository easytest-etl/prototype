package easytest.plugin.mutation;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

import easytest.shared.Constants;

@Plugin(type = Transform.PLUGIN_TYPE)
@Name(MutateTransformConfig.PLUGIN_NAME)
@Description(MutateTransformConfig.PLUGIN_DESCRIPTION)
public class MutateTransformPlugin extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MutateTransformPlugin.class);

  private final MutateTransformConfig config;

  private final Random random;

  private Schema inputSchema;

  private Map<String, String> mutations;
  private int sampleCount;

  public MutateTransformPlugin(MutateTransformConfig config) {
    this.config = config;
    this.random = new Random();
  }

  /**
   * This function is called when the pipeline is published. You should use this
   * for validating the config and setting
   * additional parameters in pipelineConfigurer.getStageConfigurer(). Those
   * parameters will be stored and will be made
   * available to your plugin during runtime via the TransformContext. Any errors
   * thrown here will stop the pipeline
   * from being published.
   * 
   * @param pipelineConfigurer Configures an ETL Pipeline. Allows adding datasets
   *                           and streams and storing parameters
   * @throws IllegalArgumentException If the config is invalid.
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);

    this.inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(this.inputSchema);

    this.config.validate();

    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    collector.getOrThrowException();
  }

  /**
   * This function is called when the pipeline has started. The values configured
   * in here will be made available to the
   * transform function. Use this for initializing costly objects and opening
   * connections that will be reused.
   * 
   * @param context Context for a pipeline stage, providing access to information
   *                about the stage, metrics, and plugins.
   * @throws Exception If there are any issues before starting the pipeline.
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    this.inputSchema = context.getInputSchema();

    if (config.mutations != null) {
      @SuppressWarnings("null")
      Map<String, String> mutations = Splitter.on(',')
          .withKeyValueSeparator(":")
          .split(config.mutations);

      this.mutations = mutations;
    }
  }

  /**
   * This is the method that is called for every record in the pipeline and allows
   * you to make any transformations
   * you need and emit one or more records to the next stage.
   * 
   * @param input   The record that is coming into the plugin
   * @param emitter An emitter allowing you to emit one or more records to the
   *                next stage
   * @throws Exception
   */
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    if (sampleCount == config.sampleSize) {
      emitter.emit(input);
      return;
    }

    List<Schema.Field> fields = inputSchema.getFields();
    StructuredRecord.Builder builder = StructuredRecord.builder(inputSchema);

    for (Schema.Field field : fields) {
      String name = field.getName();

      if (input.get(name) != null) {
        Object value = input.get(name);

        boolean apply = this.random.nextBoolean();

        if (apply) {
          if (mutations.get(name) != null) {
            value = applyMutation(mutations.get(name), value, null);
            sampleCount++;
          }
        }

        builder.set(name, value);
      }
    }

    emitter.emit(builder.build());
  }

  private Object applyMutation(String type, Object ground, String specific) {
    switch (type) {
      case Constants.MUTATION_NULL:
        return null;
      case Constants.MUTATION_SPECIFIC:
        return specific;
      case Constants.MUTATION_ZERO:
        return 0;
      case Constants.MUTATION_NEGATIVE:
        return Operators.negative(ground);
      case Constants.MUTATION_ABSOLUTE:
        return Operators.absolute(ground);
      case Constants.MUTATION_LARGE_NEGATIVE:
        return Operators.negativeMax(ground);
      case Constants.MUTATION_LARGE_POSITIVE:
        return Operators.positiveMax(ground);
      default:
        return null;
    }
  }

  /**
   * This function will be called at the end of the pipeline. You can use it to
   * clean up any variables or connections.
   */
  @Override
  public void destroy() {
    // No Op
  }
}
