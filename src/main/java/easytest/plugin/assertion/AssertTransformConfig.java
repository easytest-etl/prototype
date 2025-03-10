package easytest.plugin.assertion;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import io.cdap.cdap.api.plugin.PluginConfig;

import io.cdap.cdap.etl.api.FailureCollector;

import easytest.expression.Expression;
import easytest.expression.ExpressionException;

public class AssertTransformConfig extends PluginConfig {

  public static final String PLUGIN_NAME = "AssertTransform";
  public static final String PLUGIN_DESCRIPTION = "A plugin to provide assertions for the transformations in the pipeline.";

  private static final String FIELD_DESCRIPTION = "description";
  private static final String FIELD_EXPRESSION = "expression";

  @Name(FIELD_DESCRIPTION)
  @Description("Specifies the name for this assertion.")
  @Macro
  public final String description;

  @Name(FIELD_EXPRESSION)
  @Description("The rules are specified using jexl expressions and the variables for " +
      "expression can include values from input, fixtures, global or runtime arguments of the pipeline.")
  @Macro
  public final String expression;

  public AssertTransformConfig(String description, String expression) {
    this.description = description;
    this.expression = expression;
  }

  public void compile(Expression exp, FailureCollector collector) {
    if (containsMacro(FIELD_EXPRESSION) || this.expression == null || this.expression.isEmpty()) {
      return;
    }

    try {
      exp.create(this.expression);
      exp.validate();
    } catch (ExpressionException e) {
      collector.addFailure(String.format("Error encountered while compiling the expression : %s",
          e.getMessage()), null).withConfigProperty(FIELD_EXPRESSION);
    }
  }

  public void validate() throws IllegalArgumentException {
    // This method should be used to validate that the configuration is valid.
    if (this.description == null || this.description.isEmpty()) {
      throw new IllegalArgumentException("A description must be specified.");
    }
    if (this.expression == null || this.expression.isEmpty()) {
      throw new IllegalArgumentException("An expression cannot be empty.");
    }
    // You can use the containsMacro() function to determine if you can validate at
    // deploy time or runtime.
    // If your plugin depends on fields from the input schema being present or the
    // right type, use inputSchema
  }
}
