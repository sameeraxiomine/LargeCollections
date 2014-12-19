package com.axiomine.largecollections.serdes.basic;

import java.nio.ByteBuffer;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class DoubleSerDe {
    public static class SerFunction implements Function<Double,byte[]>{
        public byte[] apply(Double arg) {
            if(arg==null){
                return null;
            }
            else{
                byte [] bytes = Longs.toByteArray(Double.doubleToLongBits(arg));
                return bytes;
            }
        }    
    }

    
    public static class DeSerFunction implements Function<byte[],Double>{
        public Double apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return Double.longBitsToDouble(Longs.fromByteArray(arg));    
            }
        }    
    }

}
