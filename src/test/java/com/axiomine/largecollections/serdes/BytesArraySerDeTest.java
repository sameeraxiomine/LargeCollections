package com.axiomine.largecollections.serdes;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.BytesArraySerDes;

public class BytesArraySerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<byte[]> ser = new BytesArraySerDes.SerFunction();
        TurboDeSerializer<byte[]> deser = new BytesArraySerDes.DeSerFunction();
        
        String s = "This is a test";
        byte[] sba = ser.apply(s.getBytes());
        byte[] dba = deser.apply(sba);
        Assert.assertEquals(s, new String(dba));
    }
    
}
