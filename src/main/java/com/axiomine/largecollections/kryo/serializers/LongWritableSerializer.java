package com.axiomine.largecollections.kryo.serializers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class LongWritableSerializer extends Serializer<LongWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, LongWritable object) {
        output.writeLong(object.get(), false);
    }

    public LongWritable read (Kryo kryo, Input input, Class<LongWritable> type) {
        return new LongWritable(input.readLong(false));
    }
}