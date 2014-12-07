package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.ByteSerDe;

public class ByteSerDeTest {
    
    @Test
    public void test() {
        ByteSerDe.ByteSerFunction ser = new ByteSerDe.ByteSerFunction();
        ByteSerDe.ByteDeSerFunction deser = new ByteSerDe.ByteDeSerFunction();
        
        byte b = 1;
        byte[] sba = ser.apply(b);
        byte d = deser.apply(sba);
        Assert.assertEquals(b, d);

    }
    
}
