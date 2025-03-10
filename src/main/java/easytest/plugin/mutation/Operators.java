package easytest.plugin.mutation;

public class Operators {
  public static Object negative(Object ground) {
    if (ground instanceof Long) {
      return -1 * (Long) ground;
    } else if (ground instanceof Short) {
      return -1 * (Short) ground;
    } else if (ground instanceof Double) {
      return -1 * (Double) ground;
    } else if (ground instanceof Float) {
      return -1 * (Float) ground;
    } else {
      return -1 * (Integer) ground;
    }
  }

  public static Object absolute(Object ground) {
    if (ground instanceof Long) {
      return Math.abs((Long) ground);
    } else if (ground instanceof Short) {
      return Math.abs((Short) ground);
    } else if (ground instanceof Double) {
      return Math.abs((Double) ground);
    } else if (ground instanceof Float) {
      return Math.abs((Float) ground);
    } else {
      return Math.abs((Integer) ground);
    }
  }

  public static Object positiveMax(Object ground) {
    if (ground instanceof Long) {
      return Long.MAX_VALUE;
    } else if (ground instanceof Short) {
      return Short.MAX_VALUE;
    } else if (ground instanceof Double) {
      return Double.MAX_VALUE;
    } else if (ground instanceof Float) {
      return Float.MAX_VALUE;
    } else {
      return Integer.MAX_VALUE;
    }
  }

  public static Object negativeMax(Object ground) {
    return negative(positiveMax(ground));
  }
}
