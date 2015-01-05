package com.axiomine.largecollections.serdes;


public class ByteSerDes {
    public static class SerFunction implements TurboSerializer<Byte>{
        private static final long serialVersionUID = 1L;

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
    
    public static class DeSerFunction implements TurboDeSerializer<Byte>{
        private static final long serialVersionUID = 1L;
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
