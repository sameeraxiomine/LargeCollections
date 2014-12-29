package com.axiomine.largecollections.serdes;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.serdes.WritableSerDes;

public class WritableSerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<Writable> ser = new WritableSerDes.SerFunction();
        TurboDeSerializer<Writable> deser = new WritableSerDes.DeSerFunction(Text.class);

        TurboDeSerializer<Text> deser2 = new WritableSerDes.TextDeSerFunction();
        Text s = new Text("This is a test");
        byte[] sba = ser.apply(s);
        Writable ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

        
        ss = deser2.apply(sba);
        Assert.assertEquals(s, ss);
    }
    
}
