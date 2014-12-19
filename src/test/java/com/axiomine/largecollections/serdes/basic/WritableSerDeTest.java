package com.axiomine.largecollections.serdes.basic;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.serdes.WritableSerDes;

public class WritableSerDeTest {
    
    @Test
    public void test() {
        WritableSerDes.SerFunction ser = new WritableSerDes.SerFunction();
        Text t = new Text();
        WritableSerDes.DeSerFunction deser = new WritableSerDes.DeSerFunction(Text.class);
        WritableSerDes.TextDeSerFunction deser2 = new WritableSerDes.TextDeSerFunction();
        Text s = new Text("This is a test");
        byte[] sba = ser.apply(s);
        Writable ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

        
        ss = deser2.apply(sba);
        Assert.assertEquals(s, ss);
    }
    
}
