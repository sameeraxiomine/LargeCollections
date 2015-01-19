/*
 * Copyright 2014 Axiomine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.axiomine.largecollections.kryo.serializers;

import org.apache.hadoop.io.Text;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TextSerializer extends Serializer<Text> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, Text object) {
        byte[] b = object.getBytes();
        output.writeInt(b.length, true);
        output.write(b);
    }

    public Text read (Kryo kryo, Input input, Class<Text> type) {
        int len = input.readInt(true);
        return new Text(input.readBytes(len));
    }
}