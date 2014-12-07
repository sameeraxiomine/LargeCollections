package com.axiomine.largecollections.functions;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.FloatSerDe;

public class FloatSerDeTest {
    
    @Test
    public void test() {
        FloatSerDe.FloatSerFunction ser = new FloatSerDe.FloatSerFunction();
        FloatSerDe.FloatDeSerFunction deser = new FloatSerDe.FloatDeSerFunction();
        
        Float f = 1f;
        byte[] ba = ser.apply(f);
        Float ff = deser.apply(ba);
        Assert.assertEquals(f, ff);
    }
    
}
