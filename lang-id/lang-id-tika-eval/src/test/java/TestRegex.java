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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestRegex {
    private static final Pattern REGEX =
            Pattern.compile("(\\p{IsAlphabetic}+)");

    @Test
    public void test() {
        String t = "the quick 위키백과, 우리 모두의 백과사전 维基百科 brown 234fox";
        Matcher m = REGEX.matcher(t);
        while (m.find()) {
            System.out.println(m.group(1));
        }
    }
}
