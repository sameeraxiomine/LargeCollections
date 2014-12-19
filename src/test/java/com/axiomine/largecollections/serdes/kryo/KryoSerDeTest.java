package com.axiomine.largecollections.serdes.kryo;



import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.KryoSerDes;

public class KryoSerDeTest {
    
    @Test
    public void test() {
        
        KryoSerDes.SerFunction<String> ser = new KryoSerDes.SerFunction<String>();
       
        KryoSerDes.DeSerFunction<String> deser = new KryoSerDes.DeSerFunction<String>();
        String s = "This is a test";
        byte[] sba = ser.apply(s);
        String ss = (String) deser.apply(sba);
        Assert.assertEquals(s, ss);
    }
}
