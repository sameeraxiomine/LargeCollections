package com.axiomine.largecollections.serdes.basic;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.basic.LongSerDe;

public class LongSerDeTest {
    
    @Test
    public void test() {
        LongSerDe.SerFunction ser = new LongSerDe.SerFunction();
        LongSerDe.DeSerFunction deser = new LongSerDe.DeSerFunction();
        
        Long l = 1l;
        byte[] ba = ser.apply(l);
        Long ll = deser.apply(ba);
        Assert.assertEquals(l, ll);
    }
    
}