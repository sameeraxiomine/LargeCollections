package com.axiomine.largecollections.serdes;

import com.google.common.primitives.Ints;

public class IntegerSerDes2 {
    public static class SerFunction implements TurboSerializer<Integer> {
        private static final long serialVersionUID = 1094998036571475066L;
        public byte[] apply(Integer arg) {
            if (arg == null) {
                return null;
            } else {
                return Ints.toByteArray(arg);
            }
        }
    }
    
    public static class DeSerFunction implements  TurboDeSerializer<Integer> {
        private static final long serialVersionUID = -2430018069955808296L;
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
