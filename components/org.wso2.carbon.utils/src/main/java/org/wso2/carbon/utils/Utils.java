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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementPermission;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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
    @SuppressFBWarnings(
            value = "PATH_TRAVERSAL_IN",
            justification = "File path is based on system properties and constants")
    public static Path getCarbonConfigHome() {
        return Paths.get(getCarbonHome().toString(), Constants.CONF_DIR);
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
     * Returns the Runtime Home directory path. If {@code wso2.runtime.path} system property is not found, gets the
     * {@code RUNTIME_PATH_ENV} system property value and sets to the runtime home.
     *
     * @return the Runtime Home directory path
     */
    public static Path getRuntimePath() {
        String runtimeHome = System.getProperty(Constants.RUNTIME_PATH);
        if (runtimeHome == null) {
            runtimeHome = System.getenv(Constants.RUNTIME_PATH_ENV);
            System.setProperty(Constants.RUNTIME_PATH, runtimeHome);
        }
        return Paths.get(runtimeHome);
    }

    /**
     * Get the conf path of the runtime.
     *
     * @return Path to the conf dir of runtime
     */
    @SuppressFBWarnings(
            value = "PATH_TRAVERSAL_IN",
            justification = "File path is based on system properties and constants")
    public static Path getRuntimeConfigPath() {
        return Paths.get(getCarbonHome().toString(), Constants.CONF_DIR, getRuntimeName());
    }

    /**
     * get the current Runtime name.
     *
     * @return the Runtime name
     */
    public static String getRuntimeName() {
        return System.getProperty(Constants.RUNTIME);
    }

    /**
     * Replace system property holders in the property values.
     * e.g. Replace ${carbon.home} with value of the carbon.home system property.
     *
     * @param value string value to substitute
     * @return String substituted string
     */
    @SuppressFBWarnings(
            value = "CRLF_INJECTION_LOGS",
            justification = "System generated message with system property.")
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

    /**
     * Returns a list of WSO2 Carbon Runtime names.
     *
     * @return WSO2 Carbon Runtime names
     * @throws IOException if an I/O error occurs
     */
    public static List<String> getCarbonRuntimes() throws IOException {
        Path carbonRuntimeHome = getCarbonHome().resolve(Constants.PROFILE_REPOSITORY);
        Path osgiRepoPath = carbonRuntimeHome.resolve(Constants.OSGI_LIB);
        if (!Files.exists(carbonRuntimeHome)) {
            throw new IOException("invalid carbon home path " + carbonRuntimeHome.toString());
        }
        Stream<Path> runtimes = Files.list(carbonRuntimeHome);
        List<String> runtimeNames = new ArrayList<>();

        runtimes.filter(profile -> !osgiRepoPath.equals(profile))
                .filter(Objects::nonNull)
                .forEach(profile -> {
                    Path fileName = profile.getFileName();
                    if (fileName != null) {
                        runtimeNames.add(fileName.toString());
                    }
                });
        if (runtimeNames.size() == 0) {
            throw new IOException("No runtime found in path " + carbonRuntimeHome);
        }
        return runtimeNames;
    }
}
