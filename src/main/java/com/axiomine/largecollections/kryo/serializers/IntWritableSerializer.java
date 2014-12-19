package com.axiomine.largecollections.kryo.serializers;

import org.apache.hadoop.io.IntWritable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class IntWritableSerializer extends Serializer<IntWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, IntWritable object) {
        output.writeInt(object.get(), false);
    }

    public IntWritable read (Kryo kryo, Input input, Class<IntWritable> type) {
        return new IntWritable(input.readInt(false));
    }
}