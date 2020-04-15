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
package org.tallison.tika.fuzzing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class ByteInjector implements Transformer {
    Random random = new Random();
    float injectionFrequency = 0.1f;
    int maxSpan = 100;

    @Override
    public byte[] transform(byte[] input) throws IOException {
        int numInjections = (int)Math.floor((double)injectionFrequency*(double)input.length);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int[] starts = new int[numInjections];
        for (int i = 0; i < numInjections; i++) {
            starts[i] = random.nextInt(input.length-1);
        }
        Arrays.sort(starts);
        int startIndex = 0;
        for (int i = 0; i < input.length; i++) {
            bos.write(input[i]);
            if (startIndex < starts.length && starts[startIndex] == i) {
                inject(bos);
                startIndex++;
            }
        }
        return bos.toByteArray();
    }

    private void inject(ByteArrayOutputStream bos) throws IOException {
        int len = random.nextInt(maxSpan);
        byte[] randBytes = new byte[len];
        random.nextBytes(randBytes);
        bos.write(randBytes);
    }
}
