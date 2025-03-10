package easytest.shared;

import io.cdap.cdap.etl.api.StageContext;

import java.util.HashMap;
import java.util.Map;

public class SingletonSharedMap {

  public static String getUniqueMapId(StageContext context, String name) {
    return String.format("%s.%s.%s",
        context.getPipelineName(),
        Long.toHexString(context.getLogicalStartTime()),
        name);
  }

  public static SingletonSharedMap getInstance() {
    if (instance == null) {
      instance = new SingletonSharedMap();
    }
    return instance;
  }

  private static SingletonSharedMap instance = null;

  private Map<String, Map<String, Object>> maps = new HashMap<>();

  private SingletonSharedMap() {
    // no-op
  }

  public boolean hasMap(String id) {
    return this.maps.containsKey(id);
  }

  public boolean has(String mapId, String key) {
    if (!hasMap(mapId)) {
      return false;
    }
    return this.maps.get(mapId).containsKey(key);
  }

  public boolean getBoolean(String mapId, String key) {
    if (!hasMap(mapId)) {
      return false;
    }
    return (Boolean) this.maps.get(mapId).getOrDefault(key, false);
  }

  public String getString(String mapId, String key) {
    if (!hasMap(mapId)) {
      return null;
    }
    return String.valueOf(this.maps.get(mapId).get(key));
  }

  public Map<String, Object> getMap(String id) {
    if (!hasMap(id)) {
      this.maps.put(id, new HashMap<>());
    }
    return this.maps.get(id);
  }

  public void put(String mapId, String key, Object value) {
    if (!hasMap(mapId)) {
      this.maps.put(mapId, new HashMap<>());
    }
    this.maps.get(mapId).put(key, value);
  }

  public void removeMap(String id) {
    this.maps.remove(id);
  }
}
