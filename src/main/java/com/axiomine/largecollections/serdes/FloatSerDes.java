package com.axiomine.largecollections.serdes;

import com.google.common.primitives.Ints;

public class FloatSerDes {
    public static class SerFunction implements TurboSerializer<Float>{
        private static final long serialVersionUID = 5L;

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
    
    public static class DeSerFunction implements TurboDeSerializer<Float>{
        private static final long serialVersionUID = 5L;
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
