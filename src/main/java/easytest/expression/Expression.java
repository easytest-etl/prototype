package easytest.expression;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlScript;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Expression {

  private final Set<List<String>> variables;
  private final JexlEngine engine;

  private JexlScript script = null;

  public Expression(Namespace ns) {
    this.variables = new HashSet<>();

    this.engine = new JexlBuilder()
        .namespaces(ns.functions())
        .silent(false)
        .cache(1024)
        .strict(true)
        .logger(new NullLogger())
        .create();
  }

  public Set<List<String>> getVariables() {
    return this.variables;
  }

  public void create(String expression) throws ExpressionException {
    this.variables.clear();

    try {
      this.script = this.engine.createScript(expression);
      this.variables.addAll(this.script.getVariables());
    } catch (JexlException e) {
      if (e.getCause() != null) {
        throw new ExpressionException(e.getCause().getMessage());
      } else {
        throw new ExpressionException(e.getMessage());
      }
    } catch (Exception e) {
      throw new ExpressionException(e.getMessage());
    }
  }

  public void validate() throws ExpressionException {
    for (List<String> vars : this.variables) {
      try {
        VariableType type = VariableType.fromString(vars.get(0));

        switch (type) {
          case TOKEN:
            if (vars.size() != 3) {
              throw new ExpressionException("Incorrect 'token' access, a token is represented as " +
                  "token['<fixture-name>']['<key>']");
            }
            break;
          case RUNTIME:
            if (vars.size() != 2) {
              throw new ExpressionException(
                  "Incorrect 'runtime' access, a runtime is represented as runtime['<field-name>']");
            }
            break;
          case GLOBAL:
            if (vars.size() != 2) {
              throw new ExpressionException(
                  "Incorrect 'global' access, a global is represented as global['<field-name>']");
            }
            break;
          case INPUT:
            if (vars.size() != 2) {
              throw new ExpressionException(
                  "Incorrect 'input' access, input values could be accessed as input['<field-name>']");
            }
            break;
          default:
            break;
        }
      } catch (IllegalArgumentException e) {
        throw new ExpressionException(
            "Expression can only specify either 'input', 'global', 'runtime', or 'token'.");
      }
    }
  }

  public boolean evaluateAsBoolean(Context context) throws ExpressionException {
    Result result = this.execute(context);
    return result.getBoolean();
  }

  private Result execute(Context context) throws ExpressionException {
    try {
      Result variable = new Result(script.execute(context));
      return variable;
    } catch (JexlException e) {
      // Generally JexlException wraps the original exception, so it's good idea
      // to check if there is a inner exception, if there is wrap it in
      // 'DirectiveExecutionException'
      // else just print the error message.
      if (e.getCause() != null) {
        throw new ExpressionException(e.getCause().getMessage());
      } else {
        throw new ExpressionException(e.getMessage());
      }
    } catch (NumberFormatException e) {
      throw new ExpressionException("Type mismatch. Change type of constant " +
          "or convert to right data type using conversion functions available. Reason : "
          + e.getMessage());
    } catch (Exception e) {
      if (e.getCause() != null) {
        throw new ExpressionException(e.getCause().getMessage());
      } else {
        throw new ExpressionException(e.getMessage());
      }
    }
  }
}
