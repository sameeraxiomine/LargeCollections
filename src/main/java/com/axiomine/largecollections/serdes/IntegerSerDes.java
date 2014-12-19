package com.axiomine.largecollections.serdes;

import com.google.common.base.Function;
import com.google.common.primitives.Ints;

public class IntegerSerDes {
    public static class SerFunction implements Function<Integer, byte[]> {
        public byte[] apply(Integer arg) {
            if (arg == null) {
                return null;
            } else {
                return Ints.toByteArray(arg);
            }
        }
    }
    
    public static class DeSerFunction implements
            Function<byte[], Integer> {
        public Integer apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return Ints.fromByteArray(arg);    
            }
            
        }
    }
    
}
