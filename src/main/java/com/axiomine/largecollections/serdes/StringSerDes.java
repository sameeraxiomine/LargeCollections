package com.axiomine.largecollections.serdes;


public class StringSerDes {
    public static class SerFunction implements TurboSerializer<String>{
        private static final long serialVersionUID = 11L;
        public byte[] apply(String arg) {
            if(arg==null){
                return null;
            }
            else{
                return arg.getBytes();    
            }
            
        }    
    }

    public static class DeSerFunction implements TurboDeSerializer<String>{
        private static final long serialVersionUID = 11L;
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
