package easytest.plugin.fixture;

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

import easytest.shared.SingletonSharedMap;
import easytest.shared.Constants;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

@Plugin(type = Transform.PLUGIN_TYPE)
@Name(FixtureTransformConfig.PLUGIN_NAME)
@Description(FixtureTransformConfig.PLUGIN_DESCRIPTION)
public class FixtureTransformPlugin extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(FixtureTransformPlugin.class);

  private final FixtureTransformConfig config;

  private Schema inputSchema;

  private String fixtureMapId = null;
  private boolean isFixtureSetup = false;

  public FixtureTransformPlugin(FixtureTransformConfig config) {
    this.config = config;
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
    this.fixtureMapId = SingletonSharedMap.getUniqueMapId(context, config.name);
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
    if (!isFixtureSetup) {
      Map<String, Object> map = SingletonSharedMap.getInstance().getMap(fixtureMapId);

      if (config.fixtureValues != null) {
        @SuppressWarnings("null")
        Map<String, String> pairs = Splitter.on(',')
            .withKeyValueSeparator(":")
            .split(config.fixtureValues);

        map.putAll(pairs);
      }

      if (config.assertionControl != null) {
        @SuppressWarnings("null")
        Map<String, String> params = Splitter.on(',')
            .withKeyValueSeparator(":")
            .split(config.assertionControl);

        for (Entry<String, String> entry : params.entrySet()) {
          String assertionMapId = SingletonSharedMap.getUniqueMapId(getContext(), entry.getKey());

          boolean assertionEnabled = entry.getValue().equalsIgnoreCase(Constants.ENABLED);

          if (!assertionEnabled) {
            LOG.info("Fixture ({}) has disabled an assertion: {}", config.name, entry.getKey());
          }

          SingletonSharedMap.getInstance().put(assertionMapId, Constants.ENABLED, assertionEnabled);
        }
      }

      isFixtureSetup = true;
    }

    emitter.emit(input);
  }

  /**
   * This function will be called at the end of the pipeline. You can use it to
   * clean up any variables or connections.
   */
  @Override
  public void destroy() {
    SingletonSharedMap.getInstance().removeMap(fixtureMapId);
  }
}
