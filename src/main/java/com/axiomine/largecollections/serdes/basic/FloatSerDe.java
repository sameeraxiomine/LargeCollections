package com.axiomine.largecollections.serdes.basic;

import java.nio.ByteBuffer;

import com.google.common.base.Function;
import com.google.common.primitives.Ints;

public class FloatSerDe {
    public static class SerFunction implements Function<Float,byte[]>{
        public byte[] apply(Float arg) {
            if(arg==null){
                return null;
            }
            else{
                byte [] bytes = Ints.toByteArray(Float.floatToIntBits(arg));
                return bytes;
            }
        }    
    }
    
    public static class DeSerFunction implements Function<byte[],Float>{
        public Float apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return Float.intBitsToFloat(Ints.fromByteArray(arg));    
            }
            
        }    
    }


}
