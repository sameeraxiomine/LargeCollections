package com.axiomine.largecollections.serdes;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class LongSerDes {
    public static class SerFunction implements TurboSerializer<Long>{
        private static final long serialVersionUID = 9L;

        public byte[] apply(Long arg) {
            if(arg==null){
                return null;
            }
            else{
                return Longs.toByteArray(arg);    
            }
            
        }    
    }
    
    public static class DeSerFunction implements TurboDeSerializer<Long>{
        private static final long serialVersionUID = 9L;
        public Long apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return Longs.fromByteArray(arg);    
            }
            
        }    
    }


}
