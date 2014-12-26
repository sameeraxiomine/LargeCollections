package com.axiomine.largecollections.serdes;


public class BytesArraySerDes {
    public static class SerFunction implements TurboSerializer<byte[]> {
        private static final long serialVersionUID = 0L;

        public byte[] apply(byte[] arg) {
            if (arg == null) {
                return null;
            } else {
                return arg;
            }
        }
    }
    
    public static class DeSerFunction implements TurboDeSerializer<byte[]> {
        private static final long serialVersionUID = 0L;

        public byte[] apply(byte[] arg) {
            if (arg == null) {
                return null;
            } else {
                return arg;
            }
        }
    }
}
