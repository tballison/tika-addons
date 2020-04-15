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

import java.io.IOException;
import java.util.Random;

public class CompositeTransformer implements Transformer {

    Random random = new Random();

    private final int maxTransforms;
    private final Transformer[] transformers;

    public CompositeTransformer(Transformer ... transformers) {
        this(transformers.length, transformers);
    }

    public CompositeTransformer(int maxTransforms, Transformer ... transformers) {
        this.maxTransforms = maxTransforms;
        this.transformers = transformers;
    }

    @Override
    public byte[] transform(byte[] input) throws IOException {
        int transformerCount = (maxTransforms == 1) ? 1 : random.nextInt(maxTransforms-1);
        int[] transformerIndices = new int[transformerCount];
        for (int i = 0; i < transformerCount; i++) {
            transformerIndices[i] = random.nextInt(transformerCount);
        }
        byte[] currBytes = input;
        for (int i = 0; i < transformerIndices.length; i++) {
            currBytes = transformers[transformerIndices[i]].transform(currBytes);
        }
        return currBytes;
    }
}
