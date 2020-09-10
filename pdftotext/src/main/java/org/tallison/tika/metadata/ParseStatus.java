/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.tika.metadata;

import org.apache.tika.metadata.Property;

public class ParseStatus {

    //status namespace for metadata properties
    public static final String STATUS_NS = "status:";
    public enum VALIDITY {
        VALID("valid"),
        VALID_WARNINGS("valid-warnings"),
        REJECTED_UNSAFE("rejected-unsafe"),
        REJECTED_AMBIGUOUS("rejected-ambiguous"),
        REJECTED("rejected");

        private final String name;
        VALIDITY(String string) {
            this.name = string;
        }

        public String getName() {
            return name;
        }
    }

    public enum SAFETY {
        SAFE("safe"),
        SAFE_WARNINGS("safe-warnings"),
        UNSAFE_WARNINGS("unsafe-warnings"),
        UNSAFE("unsafe");

        private final String name;
        SAFETY(String string) {
            this.name = string;
        }

        public String getName() {
            return name;
        }
    }

    public static final Property VALIDITY_STATUS = Property.externalClosedChoise(
            STATUS_NS + "validity-status", VALIDITY.VALID.getName(),
            VALIDITY.VALID_WARNINGS.getName(), VALIDITY.REJECTED_UNSAFE.getName(),
            VALIDITY.REJECTED_AMBIGUOUS.getName(), VALIDITY.REJECTED.getName()
    );

    public static final Property SAFETY_STATUS = Property.externalClosedChoise(
            STATUS_NS + "safety-status", SAFETY.SAFE.getName(),
            SAFETY.SAFE_WARNINGS.getName(), SAFETY.UNSAFE_WARNINGS.getName(),
            SAFETY.UNSAFE.getName()
    );

    public static final Property WARNINGS = Property.externalTextBag(STATUS_NS+"warnings");
}
