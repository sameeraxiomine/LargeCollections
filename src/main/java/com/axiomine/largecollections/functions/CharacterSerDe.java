package com.axiomine.largecollections.functions;

import com.google.common.base.Function;

public class CharacterSerDe {
    public static class SerFunction implements Function<Character,byte[]>{
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
    
    public static class DeSerFunction implements Function<byte[],Character>{
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
