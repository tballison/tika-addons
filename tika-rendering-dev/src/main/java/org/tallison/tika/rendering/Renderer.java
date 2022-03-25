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
package org.tallison.tika.rendering;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.exception.TikaException;
import org.apache.tika.parser.ParseContext;

/**
 * Interface for a renderer.  This should be flexible enough to run on the initial design: PDF pages
 * but also on portions of PDF pages as well as on other document types.
 *
 * Not yet clear how to make those configurations against a range of different file types. Also,
 * we'll want to allow the renderer to make decisions "in the file".  E.g., there could be a
 * PDF with massive pages.  The renderer should figure out what to do with those, and we shouldn't
 * preset a page size.
 */
public interface Renderer {

    RenderResults render(InputStream is, ParseContext parseContext) throws IOException,
            TikaException;
}
