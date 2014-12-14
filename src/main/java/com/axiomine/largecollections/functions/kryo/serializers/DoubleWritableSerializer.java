package com.axiomine.largecollections.functions.kryo.serializers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class DoubleWritableSerializer extends Serializer<DoubleWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, DoubleWritable object) {
        output.writeDouble(object.get());
    }

    public DoubleWritable read (Kryo kryo, Input input, Class<DoubleWritable> type) {
        return new DoubleWritable(input.readDouble());
    }
}