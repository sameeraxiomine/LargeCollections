package com.axiomine.largecollections.serdes;

import java.io.Externalizable;

import com.axiomine.largecollections.utilities.SerDeUtils;
import com.google.common.base.Function;

public class ExternalizableSerDes {
    public static class SerFunction implements TurboSerializer<Externalizable> {
        private static final long serialVersionUID = 4L;

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

    public static class DeSerFunction implements TurboDeSerializer<Externalizable>{        
        private static final long serialVersionUID = 4L;

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
