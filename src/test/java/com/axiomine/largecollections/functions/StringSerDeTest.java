package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.StringSerDe;

public class StringSerDeTest {
    
    @Test
    public void test() {
        StringSerDe.SerFunction ser = new StringSerDe.SerFunction();
        StringSerDe.DeSerFunction deser = new StringSerDe.DeSerFunction();
        
        String s = "This is a test";
        byte[] sba = ser.apply(s);
        String ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

    }
    
}
