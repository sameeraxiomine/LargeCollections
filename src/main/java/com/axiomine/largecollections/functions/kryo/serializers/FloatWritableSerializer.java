package com.axiomine.largecollections.functions.kryo.serializers;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FloatWritableSerializer extends Serializer<FloatWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, FloatWritable object) {
        output.writeFloat(object.get());
    }

    public FloatWritable read (Kryo kryo, Input input, Class<FloatWritable> type) {
        return new FloatWritable(input.readFloat());
    }
}