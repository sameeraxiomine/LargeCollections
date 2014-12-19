package com.axiomine.largecollections.serdes.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class MyIntSerializer extends Serializer<Integer> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, Integer object) {
        output.writeInt(object, false);
    }

    public Integer read (Kryo kryo, Input input, Class<Integer> type) {
        return input.readInt(false);
    }
}