package easytest.plugin.assertion;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import io.cdap.cdap.etl.api.Arguments;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

import easytest.expression.Global;
import easytest.expression.ContextBuilder;
import easytest.expression.Expression;
import easytest.expression.ExpressionException;
import easytest.expression.VariableType;

import easytest.shared.SingletonSharedMap;
import easytest.shared.Constants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Plugin(type = Transform.PLUGIN_TYPE)
@Name(AssertTransformConfig.PLUGIN_NAME)
@Description(AssertTransformConfig.PLUGIN_DESCRIPTION)
public class AssertTransformPlugin extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(AssertTransformPlugin.class);

  private final AssertTransformConfig config;

  private final Expression expression = new Expression(() -> {
    Map<String, Object> functions = new HashMap<>();
    functions.put(null, Global.class);
    functions.put("math", Math.class);
    return functions;
  });

  private enum AssertionState {
    DEFAULT,
    ENABLED,
    DISABLED
  }

  private AssertionState state = AssertionState.DEFAULT;

  private Schema inputSchema;

  private String assertionFixtureMapId;
  private ContextBuilder builder;

  public AssertTransformPlugin(AssertTransformConfig config) {
    this.config = config;
  }

  /**
   * This function is called when the pipeline is published.
   * You should use this for validating the config and setting
   * additional parameters in pipelineConfigurer.getStageConfigurer().
   * Those parameters will be stored and will be made available
   * to your plugin during runtime via the TransformContext.
   * Any errors thrown here will stop the pipeline from being published.
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
    this.config.compile(this.expression, collector);
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

    this.assertionFixtureMapId = SingletonSharedMap.getUniqueMapId(context, context.getStageName());
    this.builder = new ContextBuilder();

    this.inputSchema = context.getInputSchema();

    FailureCollector collector = context.getFailureCollector();
    this.config.compile(this.expression, collector);
    collector.getOrThrowException();
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
    if (this.state == AssertionState.DEFAULT) {
      if (SingletonSharedMap.getInstance().has(this.assertionFixtureMapId, Constants.ENABLED)) {
        boolean enabled = SingletonSharedMap.getInstance().getBoolean(this.assertionFixtureMapId, Constants.ENABLED);
        if (!enabled) {
          LOG.warn("Assertion ({}) has been disabled by a fixture.", getContext().getStageName());
          this.state = AssertionState.DISABLED;
        } else {
          this.state = AssertionState.ENABLED;
        }
      } else {
        this.state = AssertionState.ENABLED;
      }

      if (this.state == AssertionState.ENABLED) {
        this.setup(getContext());
      }
    }

    if (this.state == AssertionState.DISABLED) {
      emitter.emit(input);
      return;
    }

    List<Schema.Field> fields = this.inputSchema.getFields();
    if (fields != null) {
      for (Schema.Field field : fields) {
        this.builder.addVariable(VariableType.INPUT, field.getName(), input.get(field.getName()));
      }
    }

    boolean assertionResult = this.expression.evaluateAsBoolean(this.builder.build());

    if (!assertionResult) {
      LOG.warn("Assertion ({}) Failed: {}", getContext().getStageName(), this.config.description);

      throw new IllegalStateException(
          String.format("Assertion (%s) Failed: %s", getContext().getStageName(), this.config.description));
    }

    emitter.emit(input);
  }

  /**
   * This function will be called at the end of the pipeline. You can use it to
   * clean up any variables or connections.
   */
  @Override
  public void destroy() {
    SingletonSharedMap.getInstance().removeMap(this.assertionFixtureMapId);
  }

  private void setup(TransformContext context) throws ExpressionException {
    for (List<String> variable : this.expression.getVariables()) {
      try {
        VariableType type = VariableType.fromString(variable.get(0));

        switch (type) {
          case INPUT:
            break;
          case GLOBAL:
            this.builder
                .addVariable(VariableType.GLOBAL, Constants.PIPELINE, context.getPipelineName())
                .addVariable(VariableType.GLOBAL, Constants.NAMESPACE, context.getNamespace())
                .addVariable(VariableType.GLOBAL, Constants.LOGICAL_START_TIME, context.getLogicalStartTime())
                .addVariable(VariableType.GLOBAL, Constants.PLUGIN, context.getStageName());
            break;
          case RUNTIME:
            Arguments arguments = context.getArguments();
            if (!arguments.has(variable.get(1))) {
              throw new ExpressionException(
                  String.format("Expression includes a runtime argument '%s' that does not exist.", variable.get(1)));
            }
            this.builder.addVariable(VariableType.RUNTIME, variable.get(1), arguments.get(variable.get(1)));
            break;
          case TOKEN:
            String fixture = variable.get(1);
            Map<String, Object> values = SingletonSharedMap.getInstance()
                .getMap(SingletonSharedMap.getUniqueMapId(context, fixture));
            this.builder.addTokens(fixture, values);
            break;
          default:
            break;
        }
      } catch (IllegalArgumentException e) {
        throw new ExpressionException(
            String.format("Invalid map variable '%s' specified. Valid map variables are " +
                "'runtime', 'token', 'global', and 'input'.", variable.get(0)));
      }
    }
  }
}
