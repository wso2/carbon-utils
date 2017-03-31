/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.wso2.carbon.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.utils.internal.DataHolder;

import java.io.IOException;
import java.lang.management.ManagementPermission;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Carbon utility methods.
 *
 * @since 1.0.0
 */
public class Utils {
    private static final Pattern varPattern = Pattern.compile("\\$\\{([^}]*)}");
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    /**
     * Remove default constructor and make it not available to initialize.
     */

    private Utils() {
        throw new AssertionError("Instantiating utility class...");
    }

    /**
     * This method will return the carbon configuration directory path.
     * i.e ${carbon.home}/conf
     *
     * @return returns the Carbon Configuration directory path
     */
    public static Path getCarbonConfigHome() {
        return Paths.get(getCarbonHome().toString(), "conf");
    }

    /**
     * Returns the Carbon Home directory path. If {@code carbon.home} system property is not found, gets the
     * {@code CARBON_HOME_ENV} system property value and sets to the carbon home.
     *
     * @return returns the Carbon Home directory path
     */
    public static Path getCarbonHome() {
        String carbonHome = System.getProperty(Constants.CARBON_HOME);
        if (carbonHome == null) {
            carbonHome = System.getenv(Constants.CARBON_HOME_ENV);
            System.setProperty(Constants.CARBON_HOME, carbonHome);
        }
        return Paths.get(carbonHome);
    }

    /**
     * Returns the Runtime Home directory path. If {@code runtime.home} system property is not found, gets the
     * {@code RUNTIME_HOME_ENV} system property value and sets to the runtime home.
     *
     * @return the Runtime Home directory path
     */
    public static Path getRuntimeHome() {
        String runtimeHome = System.getProperty(Constants.RUNTIME_HOME);
        if (runtimeHome == null) {
            runtimeHome = System.getenv(Constants.RUNTIME_HOME_ENV);
            System.setProperty(Constants.RUNTIME_HOME, runtimeHome);
        }
        return Paths.get(runtimeHome);
    }

    /**
     * get the current Runtime name.
     *
     * @return the Runtime name
     */
    public static String getRuntime() {
        return System.getProperty(Constants.RUNTIME);
    }

    /**
     * Get the runtime home path for a runtime.
     *
     * @return Path to runtime home of given runtime.
     * @throws IOException if there error while reading runtime root.
     */
    public static Path getRuntimeHome(String runtimeName) throws IOException {
        return getAllRuntimes().get(runtimeName);
    }

    /**
     * Get all the available runtime names and paths.
     *
     * @return Map of runtime names and paths.
     * @throws IOException if there error while reading runtime root.
     */
    public static Map<String, Path> getAllRuntimes() throws IOException {
        Map<String, Path> runtimes = DataHolder.getInstance().getRuntimes();
        if (runtimes == null) {
            runtimes = Files.list(getCarbonHome().resolve(Constants.RUNTIME_ROOT_DIRECTORY_NAME))
                            .collect(Collectors.toMap(path -> path.toFile().getName(), path -> path));
            //remove the lib directory
            runtimes.remove("lib");
            DataHolder.getInstance().setRuntimes(runtimes);
        }
        return runtimes;
    }

    /**
     * Replace system property holders in the property values.
     * e.g. Replace ${carbon.home} with value of the carbon.home system property.
     *
     * @param value string value to substitute
     * @return String substituted string
     */
    public static String substituteVariables(String value) {
        Matcher matcher = varPattern.matcher(value);
        boolean found = matcher.find();
        if (!found) {
            return value;
        }
        StringBuffer sb = new StringBuffer();
        do {
            String sysPropKey = matcher.group(1);
            String sysPropValue = getSystemVariableValue(sysPropKey, null);
            if (sysPropValue == null || sysPropValue.length() == 0) {
                String msg = "System property " + sysPropKey + " is not specified";
                logger.error(msg);
                throw new RuntimeException(msg);
            }
            // Due to reported bug under CARBON-14746
            sysPropValue = sysPropValue.replace("\\", "\\\\");
            matcher.appendReplacement(sb, sysPropValue);
        } while (matcher.find());
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * A utility which allows reading variables from the environment or System properties.
     * If the variable in available in the environment as well as a System property, the System property takes
     * precedence.
     *
     * @param variableName System/environment variable name
     * @param defaultValue default value to be returned if the specified system variable is not specified.
     * @return value of the system/environment variable
     */
    public static String getSystemVariableValue(String variableName, String defaultValue) {
        return getSystemVariableValue(variableName, defaultValue, Constants.PlaceHolders.class);
    }

    /**
     * A utility which allows reading variables from the environment or System properties.
     * If the variable in available in the environment as well as a System property, the System property takes
     * precedence.
     *
     * @param variableName  System/environment variable name
     * @param defaultValue  default value to be returned if the specified system variable is not specified.
     * @param constantClass Class from which the Predefined value should be retrieved if system variable and default
     *                      value is not specified.
     * @return value of the system/environment variable
     */
    public static String getSystemVariableValue(String variableName, String defaultValue, Class constantClass) {
        String value = null;
        if (System.getProperty(variableName) != null) {
            value = System.getProperty(variableName);
        } else if (System.getenv(variableName) != null) {
            value = System.getenv(variableName);
        } else {
            try {
                String constant = variableName.replaceAll("\\.", "_").toUpperCase(Locale.getDefault());
                Field field = constantClass.getField(constant);
                value = (String) field.get(constant);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                //Nothing to do
            }
            if (value == null) {
                value = defaultValue;
            }
        }
        return value;
    }

    /**
     * When the java security manager is enabled, the {@code checkSecurity} method can be used to protect/prevent
     * methods being executed by unsigned code.
     */
    public static void checkSecurity() {
        SecurityManager secMan = System.getSecurityManager();
        if (secMan != null) {
            secMan.checkPermission(new ManagementPermission("control"));
        }
    }
}
