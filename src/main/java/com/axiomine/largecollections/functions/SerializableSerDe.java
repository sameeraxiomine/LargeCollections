package com.axiomine.largecollections.functions;

import java.io.Serializable;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class SerializableSerDe {
    public static class SerializableSerFunction implements Function<Serializable, byte[]> {
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

    public static class SerializableDeSerFunction implements Function<byte[],Serializable>{
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
