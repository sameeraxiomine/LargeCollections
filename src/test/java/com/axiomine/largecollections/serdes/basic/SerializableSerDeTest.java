package com.axiomine.largecollections.serdes.basic;

import java.io.Serializable;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.axiomine.largecollections.serdes.SerializableSerDes;

public class SerializableSerDeTest {
    
    @Test
    public void test() {
        SerializableSerDes.SerFunction ser = new SerializableSerDes.SerFunction();
        Text t = new Text();
        SerializableSerDes.DeSerFunction deser = new SerializableSerDes.DeSerFunction();
        String s = "This is a test";
        byte[] sba = ser.apply(s);
        Serializable ss = deser.apply(sba);
        Assert.assertEquals(s, ss);
    }
}
