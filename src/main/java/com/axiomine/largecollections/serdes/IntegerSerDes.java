package com.axiomine.largecollections.serdes;

import com.google.common.primitives.Ints;

public class IntegerSerDes {
    public static class SerFunction implements TurboSerializer<Integer> {
        private static final long serialVersionUID = 6L;

        public byte[] apply(Integer arg) {
            if (arg == null) {
                return null;
            } else {
                return Ints.toByteArray(arg);
            }
        }
    }
    
    public static class DeSerFunction implements TurboDeSerializer<Integer> {
        private static final long serialVersionUID = 6L;
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
