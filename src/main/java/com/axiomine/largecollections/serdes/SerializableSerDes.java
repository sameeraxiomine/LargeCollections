package com.axiomine.largecollections.serdes;

import java.io.Serializable;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class SerializableSerDes {
    public static class SerFunction implements Function<Serializable, byte[]> {
        public byte[] apply(Serializable arg) {
            if(arg==null){
                return null;
            }
            else{
                byte[] ba = null;
                ba = org.apache.commons.lang.SerializationUtils
                        .serialize((Serializable) arg);
                return ba;
            }
        }
    }

    public static class DeSerFunction implements Function<byte[],Serializable>{
        public Serializable apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return (Serializable) org.apache.commons.lang.SerializationUtils.deserialize(arg);    
            }            
        }    
    }

}
