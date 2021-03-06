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

import com.axiomine.largecollections.serdes.IntegerSerDes;
import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.axiomine.largecollections.serdes.TurboSerializer;
import com.axiomine.largecollections.util.WritableKTurboVMap;
import com.axiomine.largecollections.utilities.FileSerDeUtils;
import com.google.common.base.Function;

public class WritableKTurboVMapBasicTest {
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
        WritableKTurboVMap<IntWritable,Integer> map = null;
        try {
            TurboSerializer<Integer> vser = new IntegerSerDes.SerFunction();
            TurboDeSerializer<Integer> vdeser = new IntegerSerDes.DeSerFunction();

            map = new WritableKTurboVMap<IntWritable,Integer>(dbPath, "cacheMap",IntWritable.class,vser,vdeser);
            Assert.assertTrue(map.isEmpty());
            for (int i = 0; i < 10; i++) {
                int r = map.put(new IntWritable(i), i);
                Assert.assertEquals(r, i);
            }
            Assert.assertFalse(map.isEmpty());
            Assert.assertEquals(10, map.size());
            
            int r =  map.remove(new IntWritable(0));
            Assert.assertEquals(0, r);
            
            Integer nullI = map.remove(new IntWritable(0));
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
            
            
            
            Map<IntWritable,Integer> m = new HashMap<IntWritable,Integer>();
            m.put(new IntWritable(0),0);
            m.put(new IntWritable(1),11);
            m.put(new IntWritable(2),22);
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
            Collection<Integer> vs = map.values();
            for(Integer s:vs){
                System.out.println(s);
            }
            System.out.println("-");
            Iterator<Integer> iteri = vs.iterator();
            while(iteri.hasNext()){
                System.out.println(iteri.next());
            }

            System.out.println("EntrySet");
            Set<Map.Entry<Writable, Integer>> es = map.entrySet();
            
            for(Map.Entry<Writable, Integer> e:es){
                System.out.println(e.getKey() +"="+e.getValue());
            }
            System.out.println("-");
            Iterator<Map.Entry<Writable, Integer>> iter2 = es.iterator();
            while(iter2.hasNext()){
                Map.Entry<Writable, Integer> e = iter2.next();
                System.out.println(e.getKey() +"="+e.getValue());
            }
            
            System.out.println("Now closing");
            map.close();
            boolean b = false;
            try{
                map.put(new IntWritable(0),0);    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertTrue(b);
            
            System.out.println("Now Reopening");
            map.open();
            
            b = false;
            try{
                map.put(new IntWritable(0),0);    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertFalse(b);
            
            System.out.println("First Serialize");
            FileSerDeUtils.serializeToFile(map,new File("/tmp/x.ser"));
            
            map = (WritableKTurboVMap) FileSerDeUtils.deserializeFromFile(new File("/tmp/x.ser"));
            map.clear();
            System.out.println(map.size());
            map.put(new IntWritable(0),0);    
            System.out.println(map.size());
            System.out.println("Finally Destroying");
            map.destroy();
            b = false;
            try{
                map.put(new IntWritable(0),0);    
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
