package com.axiomine.largecollections.serdes;

import java.io.Externalizable;

import com.axiomine.largecollections.utilities.SerDeUtils;
import com.google.common.base.Function;

public class ExternalizableSerDes {
    public static class SerFunction implements Function<Externalizable, byte[]> {
        public byte[] apply(Externalizable arg) {
            if(arg==null){
                return null;
            }
            else{
                byte[] ba = null;
                ba = SerDeUtils.serializeExternalizable(arg);
                return ba;
            }
        }
    }

    public static class DeSerFunction implements Function<byte[],Externalizable>{        
        public Externalizable apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return SerDeUtils.deSerializeExternalizable(arg);    
            }
            
        }    
    }
}
