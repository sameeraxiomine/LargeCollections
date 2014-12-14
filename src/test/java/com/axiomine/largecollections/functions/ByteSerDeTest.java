package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.ByteSerDe;

public class ByteSerDeTest {
    
    @Test
    public void test() {
        ByteSerDe.SerFunction ser = new ByteSerDe.SerFunction();
        ByteSerDe.DeSerFunction deser = new ByteSerDe.DeSerFunction();
        
        byte b = 1;
        byte[] sba = ser.apply(b);
        byte d = deser.apply(sba);
        Assert.assertEquals(b, d);

    }
    
}
