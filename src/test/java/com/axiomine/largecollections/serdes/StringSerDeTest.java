package com.axiomine.largecollections.serdes;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.StringSerDes;

public class StringSerDeTest {
    
    @Test
    public void test() {
        StringSerDes.SerFunction ser = new StringSerDes.SerFunction();
        StringSerDes.DeSerFunction deser = new StringSerDes.DeSerFunction();
        
        String s = "This is a test";
        byte[] sba = ser.apply(s);
        String ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

    }
    
}
