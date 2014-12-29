package com.axiomine.largecollections.serdes;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.LongSerDes;

public class LongSerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<Long> ser = new LongSerDes.SerFunction();
        TurboDeSerializer<Long> deser = new LongSerDes.DeSerFunction();
        
        Long l = 1l;
        byte[] ba = ser.apply(l);
        Long ll = deser.apply(ba);
        Assert.assertEquals(l, ll);
    }
    
}