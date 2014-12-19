package com.axiomine.largecollections.serdes.kryo;

import org.apache.hadoop.io.BooleanWritable;

import com.axiomine.largecollections.utilities.SerDeUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BooleanWritableSerializer extends Serializer<BooleanWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, BooleanWritable object) {
        byte[] ba = SerDeUtils.serializeWritable(object);
        output.writeBoolean(object.get());
    }

    public BooleanWritable read (Kryo kryo, Input input, Class<BooleanWritable> type) {
        return new BooleanWritable(input.readBoolean());
    }
}