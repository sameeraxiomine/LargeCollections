package com.axiomine.largecollections.functions.kryo.serializers;

import org.apache.hadoop.io.Text;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TextSerializer extends Serializer<Text> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, Text object) {
        byte[] b = object.getBytes();
        output.writeInt(b.length, true);
        output.write(b);
    }

    public Text read (Kryo kryo, Input input, Class<Text> type) {
        int len = input.readInt(true);
        return new Text(input.readBytes(len));
    }
}