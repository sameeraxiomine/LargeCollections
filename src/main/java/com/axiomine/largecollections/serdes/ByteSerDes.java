package com.axiomine.largecollections.serdes;

import com.google.common.base.Function;

public class ByteSerDes {
    public static class SerFunction implements Function<Byte,byte[]>{
        public byte[] apply(Byte arg) {       
            if(arg==null){
                return null;
            }
            else{
                byte[] bytes = new byte[1];
                bytes[0] = arg;
                return bytes;
            }
        }    
    }
    
    public static class DeSerFunction implements Function<byte[],Byte>{
        public Byte apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return arg[0];    
            }
            
        }    
    }

}
