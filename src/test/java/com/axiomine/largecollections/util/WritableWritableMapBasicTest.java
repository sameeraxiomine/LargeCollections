package com.axiomine.largecollections.util;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

import com.axiomine.largecollections.utilities.FileSerDeUtils;

public class WritableWritableMapBasicTest {
    private String dbPath="";
    
    @Before
    public void setup() throws Exception{
        dbPath = System.getProperty("java.io.tmpdir")+"/test/";
        File f = new File(dbPath);
        if(f.exists()){
            FileUtils.deleteDirectory(f);
        }
    }
       
    @Test
    public void test00BasicTest() {
        WritableKVMap map = null;
        try {
            map = new WritableKVMap(dbPath, "cacheMap",IntWritable.class,IntWritable.class);
            Assert.assertTrue(map.isEmpty());
            for (int i = 0; i < 10; i++) {
                IntWritable r = (IntWritable)map.put(new IntWritable(i), new IntWritable(i));
                Assert.assertEquals(r.get(), i);
            }
            Assert.assertFalse(map.isEmpty());
            Assert.assertEquals(10, map.size());
            
            IntWritable r = (IntWritable) map.remove(new IntWritable(0));
            Assert.assertEquals(0, r.get());
            
            IntWritable nullI = (IntWritable)map.remove(new IntWritable(0));
            Assert.assertNull(nullI);
            
            Assert.assertTrue(map.containsKey(new IntWritable(1)));
            Assert.assertFalse(map.containsKey(new IntWritable(0)));
            
            try {
                map.containsValue(1);
            } catch (Exception ex) {
                boolean b = ex instanceof UnsupportedOperationException;
                Assert.assertTrue(b);
            }
            Assert.assertEquals(9, map.size());
            
            
            
            Map<IntWritable,IntWritable> m = new HashMap<IntWritable,IntWritable>();
            m.put(new IntWritable(0),new IntWritable(0));
            m.put(new IntWritable(1),new IntWritable(11));
            m.put(new IntWritable(2),new IntWritable(22));
            map.putAll(m);
            Assert.assertEquals(10, map.size());
            
            System.out.println("Keys");
            Set<Writable> ks = map.keySet();
            for(Writable s:ks){
                System.out.println(s);
            }
            System.out.println("-");
            Iterator<Writable> iter = ks.iterator();
            while(iter.hasNext()){
                System.out.println(iter.next());
            }
            
            System.out.println("Values");
            Collection<Writable> vs = map.values();
            for(Writable s:vs){
                System.out.println(s);
            }
            System.out.println("-");
            iter = vs.iterator();
            while(iter.hasNext()){
                System.out.println(iter.next());
            }

            System.out.println("EntrySet");
            Set<Map.Entry<Writable, Writable>> es = map.entrySet();
            
            for(Map.Entry<Writable, Writable> e:es){
                System.out.println(e.getKey() +"="+e.getValue());
            }
            System.out.println("-");
            Iterator<Map.Entry<Writable, Writable>> iter2 = es.iterator();
            while(iter2.hasNext()){
                Map.Entry<Writable, Writable> e = iter2.next();
                System.out.println(e.getKey() +"="+e.getValue());
            }
            
            System.out.println("Now closing");
            map.close();
            boolean b = false;
            try{
                map.put(new IntWritable(0),new IntWritable(0));    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertTrue(b);
            
            System.out.println("Now Reopening");
            map.open();
            
            b = false;
            try{
                map.put(new IntWritable(0),new IntWritable(0));    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertFalse(b);
            
            System.out.println("First Serialize");
            FileSerDeUtils.serializeToFile(map,new File("c:/tmp/x.ser"));
            
            map = (WritableKVMap) FileSerDeUtils.deserializeFromFile(new File("c:/tmp/x.ser"));
            map.clear();
            System.out.println(map.size());
            map.put(new IntWritable(0),new IntWritable(0));    
            System.out.println(map.size());
            System.out.println("Finally Destroying");
            map.destroy();
            b = false;
            try{
                map.put(new IntWritable(0),new IntWritable(0));    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertTrue(b);
            
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }
    
}
