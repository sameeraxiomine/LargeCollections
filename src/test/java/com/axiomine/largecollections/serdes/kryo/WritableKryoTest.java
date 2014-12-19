package com.axiomine.largecollections.serdes.kryo;

import junit.framework.Assert;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.axiomine.largecollections.serdes.KryoSerDes;
import com.axiomine.largecollections.serdes.WritableSerDes;

public class WritableKryoTest {
    
    @Test
    public void testIntWritable() {
        KryoSerDes.SerFunction<IntWritable> ser = new KryoSerDes.SerFunction<IntWritable>();        
        KryoSerDes.DeSerFunction<IntWritable> deser = new KryoSerDes.DeSerFunction<IntWritable>();

        IntWritable i = new IntWritable(0);
        byte[] sba = ser.apply(i);
        IntWritable ss = deser.apply(sba);
        Assert.assertEquals(0, i.get());        
    }

    @Test
    public void testText() {
        KryoSerDes.SerFunction<Text> ser = new KryoSerDes.SerFunction<Text>();        
        KryoSerDes.DeSerFunction<Text> deser = new KryoSerDes.DeSerFunction<Text>();

        Text i = new Text("This is a test");
        byte[] sba = ser.apply(i);
        Text ss = deser.apply(sba);
        Assert.assertEquals("This is a test", i.toString());        
    }

}
