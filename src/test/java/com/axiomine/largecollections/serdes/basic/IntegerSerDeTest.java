package com.axiomine.largecollections.serdes.basic;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.IntegerSerDes;

public class IntegerSerDeTest {
    
    @Test
    public void test() {
        IntegerSerDes.SerFunction ser = new IntegerSerDes.SerFunction();
        IntegerSerDes.DeSerFunction deser = new IntegerSerDes.DeSerFunction();
        
        Integer i = 1;
        byte[] ba = ser.apply(i);
        Integer ii = deser.apply(ba);
        Assert.assertEquals(i, ii);
    }
    
}