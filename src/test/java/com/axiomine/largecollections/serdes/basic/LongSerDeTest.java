package com.axiomine.largecollections.serdes.basic;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.LongSerDes;

public class LongSerDeTest {
    
    @Test
    public void test() {
        LongSerDes.SerFunction ser = new LongSerDes.SerFunction();
        LongSerDes.DeSerFunction deser = new LongSerDes.DeSerFunction();
        
        Long l = 1l;
        byte[] ba = ser.apply(l);
        Long ll = deser.apply(ba);
        Assert.assertEquals(l, ll);
    }
    
}