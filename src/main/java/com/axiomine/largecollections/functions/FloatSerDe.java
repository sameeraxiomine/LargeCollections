package com.axiomine.largecollections.functions;

import java.nio.ByteBuffer;

import com.google.common.base.Function;

public class FloatSerDe {
    public static class SerFunction implements Function<Float,byte[]>{
        public byte[] apply(Float arg) {
            if(arg==null){
                return null;
            }
            else{
                byte [] bytes = ByteBuffer.allocate(4).putFloat(arg).array();
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
                return ByteBuffer.wrap(arg).getFloat();    
            }
            
        }    
    }


}
