package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.functions.WritableSerDe;

public class WritableSerDeTest {
    
    @Test
    public void test() {
        WritableSerDe.WritableSerFunction ser = new WritableSerDe.WritableSerFunction();
        Text t = new Text();
        WritableSerDe.WritableDeSerFunction deser = new WritableSerDe.WritableDeSerFunction(Text.class);
        WritableSerDe.TextWritableDeSerFunction deser2 = new WritableSerDe.TextWritableDeSerFunction();
        Text s = new Text("This is a test");
        byte[] sba = ser.apply(s);
        Writable ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

        
        ss = deser2.apply(sba);
        Assert.assertEquals(s, ss);
    }
    
}
