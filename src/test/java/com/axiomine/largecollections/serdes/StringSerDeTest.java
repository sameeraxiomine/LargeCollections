package com.axiomine.largecollections.serdes;

import java.io.Serializable;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.StringSerDes;

public class StringSerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<String> ser = new StringSerDes.SerFunction();
        TurboDeSerializer<String> deser = new StringSerDes.DeSerFunction();

        String s = "This is a test";
        byte[] sba = ser.apply(s);
        String ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

    }
    
}
