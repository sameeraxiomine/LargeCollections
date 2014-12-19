package com.axiomine.largecollections.serdes.kryo;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ByteWritableSerializer extends Serializer<ByteWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, ByteWritable object) {
        output.writeByte(object.get());
    }

    public ByteWritable read (Kryo kryo, Input input, Class<ByteWritable> type) {
        return new ByteWritable(input.readByte());
    }
}