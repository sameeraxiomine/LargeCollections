package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.functions.WritableSerDe;

public class WritableSerDeTest {
    
    @Test
    public void test() {
        WritableSerDe.SerFunction ser = new WritableSerDe.SerFunction();
        Text t = new Text();
        WritableSerDe.DeSerFunction deser = new WritableSerDe.DeSerFunction(Text.class);
        WritableSerDe.TextDeSerFunction deser2 = new WritableSerDe.TextDeSerFunction();
        Text s = new Text("This is a test");
        byte[] sba = ser.apply(s);
        Writable ss = deser.apply(sba);
        Assert.assertEquals(s, ss);

        
        ss = deser2.apply(sba);
        Assert.assertEquals(s, ss);
    }
    
}
