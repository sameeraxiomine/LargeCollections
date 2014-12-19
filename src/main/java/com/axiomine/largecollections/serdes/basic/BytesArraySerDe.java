package com.axiomine.largecollections.serdes.basic;

import com.google.common.base.Function;

public class BytesArraySerDe {
    public static class SerFunction implements Function<byte[], byte[]> {
        public byte[] apply(byte[] arg) {
            if (arg == null) {
                return null;
            } else {
                return arg;
            }
        }
    }
    
    public static class DeSerFunction implements
            Function<byte[], byte[]> {
        public byte[] apply(byte[] arg) {
            if (arg == null) {
                return null;
            } else {
                return arg;
            }
        }
    }
}
