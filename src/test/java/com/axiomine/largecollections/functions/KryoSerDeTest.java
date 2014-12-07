package com.axiomine.largecollections.functions;



import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.KryoSerDe;
import com.axiomine.largecollections.utils.KryoUtils;

public class KryoSerDeTest {
    
    @Test
    public void test() {
        KryoSerDe.KryoSerFunction<String> ser = new KryoSerDe.KryoSerFunction<String>();
       
        KryoSerDe.KryoDeSerFunction<String> deser = new KryoSerDe.KryoDeSerFunction<String>();
        String s = "This is a test";
        byte[] sba = ser.apply(s);
        String ss = (String) deser.apply(sba);
        Assert.assertEquals(s, ss);
    }
}
