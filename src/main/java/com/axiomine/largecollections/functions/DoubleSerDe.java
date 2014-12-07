package com.axiomine.largecollections.functions;

import java.nio.ByteBuffer;

import com.google.common.base.Function;

public class DoubleSerDe {
    public static class DoubleSerFunction implements Function<Double,byte[]>{
        public byte[] apply(Double arg) {
            if(arg==null){
                return null;
            }
            else{
                byte [] bytes = ByteBuffer.allocate(8).putDouble(arg).array();
                return bytes;
            }
        }    
    }

    
    public static class DoubleDeSerFunction implements Function<byte[],Double>{
        public Double apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return ByteBuffer.wrap(arg).getDouble();    
            }
        }    
    }

}
