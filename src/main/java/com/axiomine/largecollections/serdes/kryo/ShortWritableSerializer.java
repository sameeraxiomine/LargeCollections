package com.axiomine.largecollections.serdes.kryo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ShortWritableSerializer extends Serializer<ShortWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, ShortWritable object) {
        output.writeShort(object.get());
    }

    public ShortWritable read (Kryo kryo, Input input, Class<ShortWritable> type) {
        return new ShortWritable(input.readShort());
    }
}