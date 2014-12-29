package com.axiomine.largecollections.serdes;

import java.io.Serializable;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.axiomine.largecollections.serdes.SerializableSerDes;

public class SerializableSerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<Serializable> ser = new SerializableSerDes.SerFunction();
        TurboDeSerializer<Serializable> deser = new SerializableSerDes.DeSerFunction();

        String s = "This is a test";
        byte[] sba = ser.apply(s);
        Serializable ss = deser.apply(sba);
        Assert.assertEquals(s, ss);
    }
}
