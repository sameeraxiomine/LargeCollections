package com.axiomine.largecollections.functions;

import com.google.common.base.Function;

public class BytesArraySerDe {
    public static class BytesArraySerFunction implements Function<byte[], byte[]> {
        public byte[] apply(byte[] arg) {
            if (arg == null) {
                return null;
            } else {
                return arg;
            }
        }
    }
    
    public static class BytesArrayDeSerFunction implements
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
