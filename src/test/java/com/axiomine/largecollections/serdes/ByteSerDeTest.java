package com.axiomine.largecollections.serdes;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.ByteSerDes;

public class ByteSerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<Byte> ser = new ByteSerDes.SerFunction();
        TurboDeSerializer<Byte> deser = new ByteSerDes.DeSerFunction();

        
        byte b = 1;
        byte[] sba = ser.apply(b);
        byte d = deser.apply(sba);
        Assert.assertEquals(b, d);

    }
    
}
