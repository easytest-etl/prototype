package easytest.expression;

public enum VariableType {
  INPUT("input"),
  GLOBAL("global"),
  RUNTIME("runtime"),
  TOKEN("token");

  public static VariableType fromString(String value) {
    if (value.equalsIgnoreCase(INPUT.toString())) {
      return VariableType.INPUT;
    } else if (value.equalsIgnoreCase(GLOBAL.toString())) {
      return VariableType.GLOBAL;
    } else if (value.equalsIgnoreCase(RUNTIME.toString())) {
      return VariableType.RUNTIME;
    } else if (value.equalsIgnoreCase(TOKEN.toString())) {
      return VariableType.TOKEN;
    } else {
      throw new IllegalArgumentException(String.format("Unknown variable type: %s", value));
    }
  }

  private final String identifier;

  VariableType(String identifier) {
    this.identifier = identifier;
  }

  public String toString() {
    return this.identifier;
  }
}
