package xqa.commons;

import java.io.IOException;

public class Properties {
    public static String getValue(String propertyName) throws IOException {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        java.util.Properties properties = new java.util.Properties();
        properties.load(loader.getResourceAsStream("xqa.properties"));
        return properties.getProperty(propertyName);
    }
}
