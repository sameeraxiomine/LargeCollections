package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.BytesArraySerDe;

public class BytesArraySerDeTest {
    
    @Test
    public void test() {
        BytesArraySerDe.SerFunction ser = new BytesArraySerDe.SerFunction();
        BytesArraySerDe.DeSerFunction deser = new BytesArraySerDe.DeSerFunction();
        
        String s = "This is a test";
        byte[] sba = ser.apply(s.getBytes());
        byte[] dba = deser.apply(sba);
        Assert.assertEquals(s, new String(dba));
    }
    
}
