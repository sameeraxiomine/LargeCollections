package com.axiomine.largecollections.serdes;


public class CharacterSerDes {
    public static class SerFunction implements TurboSerializer<Character>{
        private static final long serialVersionUID = 2L;

        public byte[] apply(Character arg) {  
            if(arg==null){
                return null;
            }
            else{
                char c = arg;
                byte[] bytes = new byte[1*2];
                bytes[0*2] = (byte) (arg >> 8);
                bytes[0*2+1] = (byte) c;
                return bytes;
            }
        }    
    }
    
    public static class DeSerFunction implements TurboDeSerializer<Character>{
        private static final long serialVersionUID = 2L;
        public Character apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                char c = (char) ((arg[0*2] << 8) + (arg[0*2+1] & 0xFF));
                return c;
            }
        }    
    }

}
