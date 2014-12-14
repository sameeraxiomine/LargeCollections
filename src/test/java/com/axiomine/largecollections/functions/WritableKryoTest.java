package com.axiomine.largecollections.functions;

import junit.framework.Assert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.functions.WritableSerDe;

public class WritableKryoTest {
    
    @Test
    public void testIntWritable() {
        KryoSerDe.SerFunction<IntWritable> ser = new KryoSerDe.SerFunction<IntWritable>();        
        KryoSerDe.DeSerFunction<IntWritable> deser = new KryoSerDe.DeSerFunction<IntWritable>();

        IntWritable i = new IntWritable(0);
        byte[] sba = ser.apply(i);
        IntWritable ss = deser.apply(sba);
        Assert.assertEquals(0, i.get());        
    }

    @Test
    public void testText() {
        KryoSerDe.SerFunction<Text> ser = new KryoSerDe.SerFunction<Text>();        
        KryoSerDe.DeSerFunction<Text> deser = new KryoSerDe.DeSerFunction<Text>();

        Text i = new Text("This is a test");
        byte[] sba = ser.apply(i);
        Text ss = deser.apply(sba);
        Assert.assertEquals("This is a test", i.toString());        
    }

}
