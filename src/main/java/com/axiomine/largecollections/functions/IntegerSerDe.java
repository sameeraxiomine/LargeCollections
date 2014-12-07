package com.axiomine.largecollections.functions;

import com.google.common.base.Function;
import com.google.common.primitives.Ints;

public class IntegerSerDe {
    public static class IntegerSerFunction implements Function<Integer, byte[]> {
        public byte[] apply(Integer arg) {
            if (arg == null) {
                return null;
            } else {
                return Ints.toByteArray(arg);
            }
        }
    }
    
    public static class IntegerDeSerFunction implements
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
