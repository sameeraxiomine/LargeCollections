package com.axiomine.largecollections.functions.kryo.serializers;

import java.io.DataOutput;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;

import com.axiomine.largecollections.utilities.SerDeUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ArrayPrimitiveWritableSerializer extends Serializer<ArrayPrimitiveWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, ArrayPrimitiveWritable object) {
        byte[] ba = SerDeUtils.serializeWritable(object);
        output.writeInt(ba.length, true);
        output.write(ba);
    }

    public ArrayPrimitiveWritable read (Kryo kryo, Input input, Class<ArrayPrimitiveWritable> type) {
        int len = input.readInt(true);
        byte[] ba = input.readBytes(len);
        return (ArrayPrimitiveWritable) SerDeUtils.deserializeWritable(new ArrayPrimitiveWritable(), ba);
    }
}