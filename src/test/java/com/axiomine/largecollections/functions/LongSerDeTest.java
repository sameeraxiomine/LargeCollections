package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.LongSerDe;

public class LongSerDeTest {
    
    @Test
    public void test() {
        LongSerDe.LongSerFunction ser = new LongSerDe.LongSerFunction();
        LongSerDe.LongDeSerFunction deser = new LongSerDe.LongDeSerFunction();
        
        Long l = 1l;
        byte[] ba = ser.apply(l);
        Long ll = deser.apply(ba);
        Assert.assertEquals(l, ll);
    }
    
}