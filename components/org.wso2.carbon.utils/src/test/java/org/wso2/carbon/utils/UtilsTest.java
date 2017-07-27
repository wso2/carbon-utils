/*
 *  Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * This class tests the functionality of org.wso2.carbon.utils.Utils class.
 *
 * @since 5.0.0
 */
public class UtilsTest {

    private static final String OS_NAME_KEY = "os.name";
    private static final String WINDOWS_PARAM = "indow";

    @Test
    public void testSubstituteVarsSystemPropertyNotNull() {
        String carbonHome = System.getProperty(Constants.CARBON_HOME);
        boolean isCarbonHomeChanged = false;

        if (carbonHome == null) {
            carbonHome = "test-carbon-home";
            System.setProperty(Constants.CARBON_HOME, carbonHome);
            isCarbonHomeChanged = true;
        }

        Assert.assertEquals(Utils.substituteVariables("${carbon.home}"), carbonHome);

        if (isCarbonHomeChanged) {
            System.clearProperty(Constants.CARBON_HOME);
        }
    }

    @Test
    public void testValueSubstituteVariables() {
        String carbonHome = System.getProperty(Constants.CARBON_HOME);
        boolean isCarbonHomeChanged = false;

        if (carbonHome == null) {
            carbonHome = "test-carbon-home";
            System.setProperty(Constants.CARBON_HOME, carbonHome);
            isCarbonHomeChanged = true;
        }

        Assert.assertEquals(Utils.substituteVariables("ValueNotExist"), "ValueNotExist");
        if (isCarbonHomeChanged) {
            System.clearProperty(Constants.CARBON_HOME);
        }
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testSubstituteVarsSystemPropertyIsNull() {
        String carbonHome = System.getProperty(Constants.CARBON_HOME);
        boolean isCarbonHomeChanged = false;

        if (carbonHome != null) {
            System.clearProperty(Constants.CARBON_HOME);
            isCarbonHomeChanged = true;
        }

        try {
            Utils.substituteVariables("${carbon.home}");
        } finally {
            if (isCarbonHomeChanged) {
                System.setProperty(Constants.CARBON_HOME, carbonHome);
            }
        }
    }

    @Test
    public void testGetSystemVariableValue() {

        Assert.assertEquals(Utils.getSystemVariableValue("testEnvironmentVariable", null), "EnvironmentVariable");
        Assert.assertEquals(Utils.getSystemVariableValue("${server.key.not.exist}", null, Constants.PlaceHolders.class),
                null);
        Assert.assertEquals(Utils.getSystemVariableValue("server.key", null, Constants.PlaceHolders.class),
                "carbon-kernel");
    }

    @DataProvider(name = "paths")
    public Object[][] createPaths() {
        return new Object[][]{{"/home/wso2/wso2carbon", "/"},
                {"C:\\Users\\WSO2\\Desktop\\CARBON~1\\WSO2CA~1.0-S", "\\"}};
    }

    @Test(dataProvider = "paths")
    public void testPathSubstitution(String carbonHome, String pathSeparator) {
        System.setProperty(Constants.CARBON_HOME, carbonHome);
        String config = "${" + Constants.CARBON_HOME + "}" + pathSeparator + "deployment" + pathSeparator;
        Assert.assertEquals(Utils.substituteVariables(config),
                carbonHome + pathSeparator + "deployment" + pathSeparator);
    }

    @Test
    public void testGetServerRuntimes() throws IOException {
        Path carbonHomePath = getResourcePath("carbon-home");
        Assert.assertNotNull(carbonHomePath, "Carbon Home cannot be null");
        System.setProperty(Constants.CARBON_HOME, carbonHomePath.toString());
        List<String> runtimesList = Utils.getCarbonRuntimes();
        Assert.assertEquals(runtimesList.size(), 1);
        Assert.assertEquals(runtimesList.get(0), "default");
    }

    @Test(expectedExceptions = IOException.class)
    public void testGetRuntimesWithInvalidPath() throws IOException {
        Path carbonHomePath = getResourcePath("carbon-home1");
        Assert.assertNotNull(carbonHomePath, "Carbon Home cannot be null");
        System.setProperty(Constants.CARBON_HOME, carbonHomePath.toString());
        List<String> runtimesList = Utils.getCarbonRuntimes();
    }



    /**
     * Get the path of a provided resource.
     *
     * @param resourcePaths path strings to the location of the resource
     * @return path of the resources
     */
    private static Path getResourcePath(String... resourcePaths) {
        URL resourceURL = UtilsTest.class.getClassLoader().getResource("");
        if (resourceURL != null) {
            String resourcePath = resourceURL.getPath();
            if (resourcePath != null) {
                resourcePath = System.getProperty(OS_NAME_KEY).contains(WINDOWS_PARAM) ?
                        resourcePath.substring(1) : resourcePath;
                return Paths.get(resourcePath, resourcePaths);
            }
        }
        return null; // Resource do not exist
    }
}
