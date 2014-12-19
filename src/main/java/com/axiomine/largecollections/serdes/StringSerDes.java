package com.axiomine.largecollections.serdes;

import com.google.common.base.Function;

public class StringSerDes {
    public static class SerFunction implements Function<String,byte[]>{
        public byte[] apply(String arg) {
            if(arg==null){
                return null;
            }
            else{
                return arg.getBytes();    
            }
            
        }    
    }

    public static class DeSerFunction implements Function<byte[],String>{
        public String apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return new String(arg);    
            }            
        }    
    }

}
