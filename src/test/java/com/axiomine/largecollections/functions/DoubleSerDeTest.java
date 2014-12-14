package com.axiomine.largecollections.functions;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.DoubleSerDe;

public class DoubleSerDeTest {
    
    @Test
    public void test() {
        DoubleSerDe.SerFunction ser = new DoubleSerDe.SerFunction();
        DoubleSerDe.DeSerFunction deser = new DoubleSerDe.DeSerFunction();
        
        double d = 1d;
        byte[] ba = ser.apply(d);
        double dd = deser.apply(ba);
        Assert.assertEquals(d, dd);
    }
    
}
