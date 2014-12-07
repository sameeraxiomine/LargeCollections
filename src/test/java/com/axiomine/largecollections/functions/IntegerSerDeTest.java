package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.IntegerSerDe;

public class IntegerSerDeTest {
    
    @Test
    public void test() {
        IntegerSerDe.IntegerSerFunction ser = new IntegerSerDe.IntegerSerFunction();
        IntegerSerDe.IntegerDeSerFunction deser = new IntegerSerDe.IntegerDeSerFunction();
        
        Integer i = 1;
        byte[] ba = ser.apply(i);
        Integer ii = deser.apply(ba);
        Assert.assertEquals(i, ii);
    }
    
}