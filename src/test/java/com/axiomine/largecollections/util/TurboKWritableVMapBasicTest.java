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
import com.axiomine.largecollections.util.TurboKWritableVMap;
import com.axiomine.largecollections.util.WritableKTurboVMap;
import com.axiomine.largecollections.utilities.FileSerDeUtils;
import com.google.common.base.Function;

public class TurboKWritableVMapBasicTest {
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
        TurboKWritableVMap<Integer,IntWritable> map = null;
        try {
            TurboSerializer<Integer> kser = new IntegerSerDes.SerFunction();
            TurboDeSerializer<Integer> kdeser = new IntegerSerDes.DeSerFunction();

            map = new TurboKWritableVMap<Integer,IntWritable>(dbPath, "cacheMap",IntWritable.class,kser,kdeser);
            Assert.assertTrue(map.isEmpty());
            for (int i = 0; i < 10; i++) {
                IntWritable r = (IntWritable)map.put(i,new IntWritable(i));
                Assert.assertEquals(r.get(), i);
            }
            Assert.assertFalse(map.isEmpty());
            Assert.assertEquals(10, map.size());
            
            IntWritable r =  (IntWritable) map.remove(0);
            Assert.assertEquals(0, r.get());
            
            r = (IntWritable) map.remove(0);
            Assert.assertNull(r);
            
            Assert.assertTrue(map.containsKey(1));
            Assert.assertFalse(map.containsKey(0));
            
            try {
                map.containsValue(1);
            } catch (Exception ex) {
                boolean b = ex instanceof UnsupportedOperationException;
                Assert.assertTrue(b);
            }
            Assert.assertEquals(9, map.size());
            
            
            
            Map<Integer,IntWritable> m = new HashMap<Integer,IntWritable>();
            m.put(0,new IntWritable(0));
            m.put(1,new IntWritable(11));
            m.put(2,new IntWritable(22));
            map.putAll(m);
            Assert.assertEquals(10, map.size());
            
            System.out.println("Keys");
            Set<Integer> ks = map.keySet();
            for(Integer s:ks){
                System.out.println(s);
            }
            System.out.println("-");
            Iterator<Integer> iter = ks.iterator();
            while(iter.hasNext()){
                System.out.println(iter.next());
            }
            
            System.out.println("Values");
            Collection<Writable> vs = map.values();
            for(Writable s:vs){
                System.out.println(((IntWritable)s).get());
            }
            System.out.println("-");
            Iterator<Writable> iteri = vs.iterator();
            while(iteri.hasNext()){
                System.out.println(iteri.next());
            }

            System.out.println("EntrySet");
            Set<Map.Entry<Integer,Writable>> es = map.entrySet();
            
            for(Map.Entry<Integer,Writable> e:es){
                System.out.println(e.getKey() +"="+e.getValue());
            }
            System.out.println("-");
            Iterator<Map.Entry<Integer,Writable>> iter2 = es.iterator();
            while(iter2.hasNext()){
                Map.Entry<Integer,Writable> e = iter2.next();
                System.out.println(e.getKey() +"="+e.getValue());
            }
            
            System.out.println("Now closing");
            map.close();
            boolean b = false;
            try{
                map.put(0,new IntWritable(0));    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertTrue(b);
            
            System.out.println("Now Reopening");
            map.open();
            
            b = false;
            try{
                map.put(0,new IntWritable(0));    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertFalse(b);
            
            System.out.println("First Serialize");
            FileSerDeUtils.serializeToFile(map,new File(dbPath+"/x.ser"));
            
            map = (TurboKWritableVMap) FileSerDeUtils.deserializeFromFile(new File(dbPath+"/x.ser"));
            map.clear();
            System.out.println(map.size());
            map.put(0,new IntWritable(0));    
            System.out.println(map.size());
            System.out.println("Finally Destroying");
            map.destroy();
            b = false;
            try{
                map.put(0,new IntWritable(0));    
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
