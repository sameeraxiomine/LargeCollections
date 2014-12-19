package com.axiomine.largecollections.serdes.basic;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class LongSerDe {
    public static class SerFunction implements Function<Long,byte[]>{
        public byte[] apply(Long arg) {
            if(arg==null){
                return null;
            }
            else{
                return Longs.toByteArray(arg);    
            }
            
        }    
    }
    
    public static class DeSerFunction implements Function<byte[],Long>{
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
