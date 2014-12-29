package com.axiomine.largecollections.serdes;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.FloatSerDes;

public class FloatSerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<Float> ser = new FloatSerDes.SerFunction();
        TurboDeSerializer<Float> deser = new FloatSerDes.DeSerFunction();
        
        Float f = 1f;
        byte[] ba = ser.apply(f);
        Float ff = deser.apply(ba);
        Assert.assertEquals(f, ff);
    }
    
}
