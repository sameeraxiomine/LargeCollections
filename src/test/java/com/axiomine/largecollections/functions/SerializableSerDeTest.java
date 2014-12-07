package com.axiomine.largecollections.functions;

import java.io.Serializable;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.axiomine.largecollections.functions.SerializableSerDe;

public class SerializableSerDeTest {
    
    @Test
    public void test() {
        SerializableSerDe.SerializableSerFunction ser = new SerializableSerDe.SerializableSerFunction();
        Text t = new Text();
        SerializableSerDe.SerializableDeSerFunction deser = new SerializableSerDe.SerializableDeSerFunction();
        String s = "This is a test";
        byte[] sba = ser.apply(s);
        Serializable ss = deser.apply(sba);
        Assert.assertEquals(s, ss);
    }
}
