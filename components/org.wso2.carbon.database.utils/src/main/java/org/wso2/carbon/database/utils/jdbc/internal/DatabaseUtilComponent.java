/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.database.utils.jdbc.internal;

import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class informs whether the OSGi bundle is activated or not.
 */
@Component(name = "org.wso2.carbon.database.utils.internal.DatabaseUtilsComponent", immediate = true)
public class DatabaseUtilComponent {

    private static final Logger log = LoggerFactory.getLogger(DatabaseUtilComponent.class);

    protected void activate(ComponentContext context) {

        if (log.isDebugEnabled()) {
            log.debug("Carbon database utils bundle activated.");
        }
    }

    protected void deactivate(ComponentContext context) {

        if (log.isDebugEnabled()) {
            log.debug("Carbon database utils bundle deactivated.");
        }
    }

}
