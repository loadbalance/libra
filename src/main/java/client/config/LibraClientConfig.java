package client.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Config for libra client
 * @author  xccui
 * Date: 13-10-9
 * Time: 8:07
 */
public class LibraClientConfig {
    public static Map<String, String> propertyMap = new HashMap<>();
    private static Map<String, String> defaultPropertyMap = new HashMap<>();

    public static final String RETRY_TIMES_KEY = "retryTimes";
    private static final String RETRY_TIMES_DEFAULT = "5";
    public static final String RETRY_INTERVAL_KEY = "retryInterval";
    private static final String RETRY_INTERVAL_DEFAULT = "5000";



    static {
        defaultPropertyMap.put(RETRY_TIMES_KEY, RETRY_TIMES_DEFAULT);
        defaultPropertyMap.put(RETRY_INTERVAL_KEY, RETRY_INTERVAL_DEFAULT);
    }

    public static String getProperty(String propertyKey) {
        String value = propertyMap.get(propertyKey);
        if (null == value) {
            value = defaultPropertyMap.get(propertyKey);
        }
        return value;
    }

    public static int getIntProperty(String propertyKey) {
        String value = propertyMap.get(propertyKey);
        if (null == value) {
            value = defaultPropertyMap.get(propertyKey);
        }
        return Integer.valueOf(value);
    }
}
