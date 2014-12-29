package com.axiomine.largecollections.serdes;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.IntegerSerDes;

public class IntegerSerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<Integer> ser = new IntegerSerDes.SerFunction();
        TurboDeSerializer<Integer> deser = new IntegerSerDes.DeSerFunction();
        
        Integer i = 1;
        byte[] ba = ser.apply(i);
        Integer ii = deser.apply(ba);
        Assert.assertEquals(i, ii);
    }
    
}