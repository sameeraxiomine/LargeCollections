package com.axiomine.largecollections.functions.kryo.serializers;

import java.io.DataOutput;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import com.axiomine.largecollections.utilities.SerDeUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class MapWritableSerializer extends Serializer<MapWritable> {
    {
        setImmutable(true);
    }

    public void write (Kryo kryo, Output output, MapWritable object) {
        byte[] ba = SerDeUtils.serializeWritable(object);
        output.writeInt(ba.length, true);
        output.write(ba);
    }

    public MapWritable read (Kryo kryo, Input input, Class<MapWritable> type) {
        int len = input.readInt(true);
        byte[] ba = input.readBytes(len);
        return (MapWritable) SerDeUtils.deserializeWritable(new MapWritable(), ba);
    }
}