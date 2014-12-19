package com.axiomine.largecollections.serdes.basic;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.basic.IntegerSerDe;

public class IntegerSerDeTest {
    
    @Test
    public void test() {
        IntegerSerDe.SerFunction ser = new IntegerSerDe.SerFunction();
        IntegerSerDe.DeSerFunction deser = new IntegerSerDe.DeSerFunction();
        
        Integer i = 1;
        byte[] ba = ser.apply(i);
        Integer ii = deser.apply(ba);
        Assert.assertEquals(i, ii);
    }
    
}