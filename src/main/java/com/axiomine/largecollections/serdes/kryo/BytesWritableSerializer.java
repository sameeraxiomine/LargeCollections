package com.axiomine.largecollections.serdes.kryo;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class BytesWritableSerializer extends Serializer<BytesWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, BytesWritable object) {
        output.writeInt(object.getLength(), true);
        output.write(object.getBytes());
    }

    public BytesWritable read (Kryo kryo, Input input, Class<BytesWritable> type) {
        int len = input.readInt(true);
        return new BytesWritable(input.readBytes(len));
    }
}