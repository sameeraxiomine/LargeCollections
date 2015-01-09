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

        
        Text s = new Text("This is a test");
        byte[] sba = ser.apply(s);
        Writable ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

        
        TurboSerializer<Text> ser2 = new WritableSerDes.TextSerFunction();
        TurboDeSerializer<Text> deser2 = new WritableSerDes.TextDeSerFunction();
        ss = deser2.apply(ser2.apply(s));
        Assert.assertEquals(s, ss);
    }
    
}
