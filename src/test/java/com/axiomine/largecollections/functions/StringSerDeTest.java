package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.StringSerDe;

public class StringSerDeTest {
    
    @Test
    public void test() {
        StringSerDe.StringSerFunction ser = new StringSerDe.StringSerFunction();
        StringSerDe.StringDeSerFunction deser = new StringSerDe.StringDeSerFunction();
        
        String s = "This is a test";
        byte[] sba = ser.apply(s);
        String ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

    }
    
}
