package easytest.expression;

import org.apache.commons.jexl3.JexlContext;

import java.util.HashMap;
import java.util.Map;

public final class Context implements JexlContext {

  private final Map<String, Object> values = new HashMap<>();

  public Context() {
    // no-op
  }

  public Context(String name, Object object) {
    this.values.put(name, object);
  }

  public Context(Map<String, Object> values) {
    this.values.putAll(values);
  }

  public Context add(String name, Object value) {
    this.values.put(name, value);
    return this;
  }

  @Override
  public Object get(String name) {
    return this.values.get(name);
  }

  @Override
  public void set(String name, Object value) {
    this.values.put(name, value);
  }

  @Override
  public boolean has(String name) {
    return this.values.containsKey(name);
  }
}
