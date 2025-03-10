package easytest.expression;

public final class Global {

  private Global() {
    // no-op
  }

  /**
   * Converts String value to double.
   *
   * @param value of type String to be converted to double.
   * @return double value of the string passed.
   */
  public static double toDouble(String value) {
    return Double.parseDouble(value);
  }

  /**
   * Coverts a String value to float.
   *
   * @param value of type string to be converted to float.
   * @return float value of the string passed.
   */
  public static float toFloat(String value) {
    return Float.parseFloat(value);
  }

  /**
   * Converts a String value to Long.
   *
   * @param value of type string to be converted to float.
   * @return float value of the string passed.
   */
  public static long toLong(String value) {
    return Long.parseLong(value);
  }

  /**
   * Converts a String value to integer.
   *
   * @param value of type string to be converted to integer.
   * @return integer value of the string passed.
   */
  public static int toInteger(String value) {
    return Integer.parseInt(value);
  }

  /**
   * Converts a String value to byte array.
   *
   * @param value of type string to be converted to byte array.
   * @return byte array value of the string passed.
   */
  public static byte[] toBytes(String value) {
    return value.getBytes();
  }

  /**
   * Concats two string without any separator in between.
   *
   * @param a First string
   * @param b Second String
   * @return concated Strings
   */
  public static String concat(String a, String b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return a.concat(b);
  }

  /**
   * Concats two string with a delimiter.
   *
   * @param a     first string.
   * @param delim delimiter.
   * @param b     second string.
   * @return concated string.
   */
  public static String concat(String a, String delim, String b) {
    if (a == null && b != null) {
      return delim.concat(b);
    } else if (b == null && a != null) {
      return a.concat(delim);
    } else if (a != null && b != null) {
      return a.concat(delim).concat(b);
    }
    return delim;
  }

  /**
   * Finds the first non-null object.
   *
   * @param objects to be check for null.
   * @return first non-null object.
   */
  public static Object coalesce(Object... objects) {
    for (Object object : objects) {
      if (object != null) {
        return object;
      }
    }
    return objects.length > 0 ? objects[0] : null;
  }

  /**
   * Finds the last non-null object.
   *
   * @param objects to be check for null.
   * @return first non-null object.
   */
  public static Object rcoalesce(Object... objects) {
    int idx = objects.length - 1;
    while (idx >= 0) {
      if (objects[idx] != null) {
        return objects[idx];
      }
      idx = idx - 1;
    }
    return objects.length > 0 ? objects[objects.length - 1] : null;
  }

  /**
   * Returns the length of the string.
   *
   * @param str for which we need to determine the length.
   * @return length of string if not null, 0 otherwise.
   */
  public static int strlen(String str) {
    if (str != null) {
      return str.length();
    }
    return 0;
  }

  /**
   * Checks if the object is null.
   *
   * @param object to be checked for null.
   * @return true if
   */
  public static boolean isnull(Object object) {
    return object == null ? true : false;
  }

  /**
   * Checks if the string is empty or not.
   *
   * @param str to be checked for empty.
   * @return true if not null and empty, else false.
   */
  public static boolean isempty(String str) {
    if (str != null && str.isEmpty()) {
      return true;
    }
    return false;
  }

  /**
   * Formats the string in way similar to String format.
   *
   * @param str  format of string.
   * @param args arguments to included in the string.
   * @return A formatted string.
   */
  public static String format(String str, Object... args) {
    return String.format(str, args);
  }

  /**
   * This String util method removes single or double quotes
   * from a string if its quoted.
   * for input string = "mystr1" output will be = mystr1
   * for input string = 'mystr2' output will be = mystr2
   *
   * @param string value to be unquoted.
   * @return value unquoted, null if input is null.
   *
   */
  public static String unquote(String string) {
    if (string != null && ((string.startsWith("\"") && string.endsWith("\""))
        || (string.startsWith("'") && string.endsWith("'")))) {
      string = string.substring(1, string.length() - 1);
    }
    return string;
  }
}
