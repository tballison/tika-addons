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
package org.tallison.tika.parser.forkrecursive;

import org.apache.tika.exception.TikaException;

/**
 * Something went so wrong with the parse, that the child process has to be
 * restarted.  This signifies either an OOM or a timeout.
 */
public class FatalTikaParseException extends TikaException {

    public FatalTikaParseException(String msg) {
        super(msg);
    }

    public FatalTikaParseException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
