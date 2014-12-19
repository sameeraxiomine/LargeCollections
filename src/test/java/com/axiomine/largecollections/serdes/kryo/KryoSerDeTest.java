package com.axiomine.largecollections.serdes.kryo;



import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.basic.KryoSerDe;

public class KryoSerDeTest {
    
    @Test
    public void test() {
        
        KryoSerDe.SerFunction<String> ser = new KryoSerDe.SerFunction<String>();
       
        KryoSerDe.DeSerFunction<String> deser = new KryoSerDe.DeSerFunction<String>();
        String s = "This is a test";
        byte[] sba = ser.apply(s);
        String ss = (String) deser.apply(sba);
        Assert.assertEquals(s, ss);
    }
}
