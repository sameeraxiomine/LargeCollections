package com.axiomine.largecollections.serdes;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.axiomine.largecollections.utilities.*;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

public class WritableSerDes {
    public static class SerFunction implements TurboSerializer<Writable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(Writable arg) {
            if (arg == null) {
                return null;
            }
            else{
                return SerDeUtils.serializeWritable(arg);
            }
        }    
    }
    
    public static class TextSerFunction implements TurboSerializer<Text>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(Text arg) {
            if (arg == null) {
                return null;
            }
            else{
                return arg.toString().getBytes();
            }
        }    
    }

    public static class ArrayPrimitiveWritableSerFunction implements TurboSerializer<ArrayPrimitiveWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(ArrayPrimitiveWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                return SerDeUtils.serializeWritable(arg);
            }
        }    
    }

    
    public static class BooleanWritableSerFunction implements TurboSerializer<BooleanWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(BooleanWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                boolean b = arg.get();
                byte[] ba = new byte[1]; 
                if(b){
                    return Ints.toByteArray(1);
                }
                else{
                    return Ints.toByteArray(0);
                }
                
            }
        }    
    }

    public static class BytesWritableSerFunction implements TurboSerializer<BytesWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(BytesWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                return arg.get();
            }
        }    
    }

    public static class ByteWritableSerFunction implements TurboSerializer<ByteWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(ByteWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                byte[] ba =  {arg.get()};
                return ba;
            }
        }    
    }

    public static class DoubleWritableSerFunction implements TurboSerializer<DoubleWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(DoubleWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                byte [] bytes = Longs.toByteArray(Double.doubleToLongBits(arg.get()));
                return bytes;

            }
        }    
    }

    public static class FloatWritableSerFunction implements TurboSerializer<FloatWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(FloatWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                byte [] bytes = Ints.toByteArray(Float.floatToIntBits(arg.get()));
                return bytes;

            }
        }    
    }

    public static class IntWritableSerFunction implements TurboSerializer<IntWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(IntWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                return Ints.toByteArray(arg.get());

            }
        }    
    }

    public static class LongWritableSerFunction implements TurboSerializer<LongWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(LongWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                return Longs.toByteArray(arg.get());
            }
        }    
    }

    public static class MapWritableSerFunction implements TurboSerializer<MapWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(MapWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                return SerDeUtils.serializeWritable(arg);
            }
        }    
    }

    public static class ShortWritableSerFunction implements TurboSerializer<ShortWritable>{
        private static final long serialVersionUID = 12L;
        public byte[] apply(ShortWritable arg) {
            if (arg == null) {
                return null;
            }
            else{
                return Shorts.toByteArray(arg.get());
            }
        }    
    }


    public static class DeSerFunction implements TurboDeSerializer<Writable>{
        private static final long serialVersionUID = 12L;
        private Class<? extends Writable> writableCls = null; 
        public DeSerFunction(Class<? extends Writable> wCls){
            this.writableCls = wCls;
        }
        public DeSerFunction(String sCls){
            try{
                this.writableCls = (Class<? extends Writable>) Class.forName(sCls);    
            }
            catch(Exception ex){
                throw Throwables.propagate(ex);
            }
            
        }
        public Writable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                Writable w=null;
                try {
                    w = this.writableCls.newInstance();
                    return SerDeUtils.deserializeWritable(w,arg);
                } catch (InstantiationException ex) {
                    Throwables.propagate(ex);
                }   
                catch(IllegalAccessException ex){
                    Throwables.propagate(ex);
                }
            
                return null;
                
            }
        }    

    }
    
    
    public static class IntWritableDeSerFunction implements TurboDeSerializer<IntWritable>{
        private static final long serialVersionUID = 1L;
        public IntWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                int i = Ints.fromByteArray(arg);
                return new IntWritable(i);
            }
        }    
    }
    public static class ArrayPrimitiveWritableDeSerFunction implements TurboDeSerializer<ArrayPrimitiveWritable>{
        private static final long serialVersionUID = 1L;

        public ArrayPrimitiveWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                ArrayPrimitiveWritable w = new ArrayPrimitiveWritable();
                return (ArrayPrimitiveWritable) SerDeUtils.deserializeWritable(w, arg);
            }
        }    
    }

    public static class BooleanWritableDeSerFunction implements TurboDeSerializer<BooleanWritable>{
        private static final long serialVersionUID = 1L;

        public BooleanWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                int i = Ints.fromByteArray(arg);
                if(i==1){
                    return new BooleanWritable(true);
                }
                else{
                    return new BooleanWritable(false);
                }
            }
        }    
    }

    public static class BytesWritableDeSerFunction implements TurboDeSerializer<BytesWritable>{
        private static final long serialVersionUID = 1L;

        public BytesWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                return new BytesWritable(arg);
            }
        }    
    }

    public static class ByteWritableDeSerFunction implements TurboDeSerializer<ByteWritable>{
        private static final long serialVersionUID = 1L;

        public ByteWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                return new ByteWritable(arg[0]);
            }
        }    
    }

    public static class DoubleWritableDeSerFunction implements TurboDeSerializer<DoubleWritable>{
        private static final long serialVersionUID = 1L;

        public DoubleWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                return new DoubleWritable(Double.longBitsToDouble(Longs.fromByteArray(arg)));
            }
        }    
    }
    public static class FloatWritableDeSerFunction implements TurboDeSerializer<FloatWritable>{
        private static final long serialVersionUID = 1L;

        public FloatWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                return new FloatWritable(Float.intBitsToFloat(Ints.fromByteArray(arg)));
            }
        }    
    }

    public static class LongWritableDeSerFunction implements TurboDeSerializer<LongWritable>{
        private static final long serialVersionUID = 1L;

        public LongWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                LongWritable w = new LongWritable();
                return (LongWritable) SerDeUtils.deserializeWritable(w, arg);
            }
        }    
    }
    
    public static class MapWritableDeSerFunction implements TurboDeSerializer<MapWritable>{
        private static final long serialVersionUID = 1L;

        public MapWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                MapWritable w = new MapWritable();
                return (MapWritable) SerDeUtils.deserializeWritable(w, arg);
            }
        }    
    }

    public static class ShortWritableDeSerFunction implements TurboDeSerializer<ShortWritable>{
        private static final long serialVersionUID = 1L;

        public ShortWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                return new ShortWritable(Shorts.fromByteArray(arg));
            }
        }    
    }

    public static class TextDeSerFunction implements TurboDeSerializer<Text>{
        private static final long serialVersionUID = 1L;

        public Text apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                return new Text(arg);
            }
        }    
    }
    
}
