package easytest.expression;

import java.util.HashMap;
import java.util.Map;

public final class ContextBuilder {

  private final Map<String, Object> input;
  private final Map<String, Object> globals;
  private final Map<String, Object> runtime;
  private final Map<String, Map<String, Object>> tokens;

  public ContextBuilder() {
    this.input = new HashMap<>();
    this.globals = new HashMap<>();
    this.runtime = new HashMap<>();
    this.tokens = new HashMap<>();
  }

  public ContextBuilder addVariable(VariableType type, String name, Object value) {
    switch (type) {
      case INPUT:
        this.input.put(name, value);
        break;
      case GLOBAL:
        this.globals.put(name, value);
        break;
      case RUNTIME:
        this.runtime.put(name, value);
        break;
      default:
        break;
    }
    return this;
  }

  public ContextBuilder addTokens(String key, Map<String, Object> values) {
    this.tokens.put(key, values);
    return this;
  }

  public Context build() {
    Context context = new Context()
        .add(VariableType.INPUT.toString(), this.input)
        .add(VariableType.GLOBAL.toString(), this.globals)
        .add(VariableType.RUNTIME.toString(), this.runtime)
        .add(VariableType.TOKEN.toString(), this.tokens);

    return context;
  }
}
