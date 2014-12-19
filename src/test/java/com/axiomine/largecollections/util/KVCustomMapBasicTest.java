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
import org.junit.Test;


import com.axiomine.largecollections.util.FastKVMap;
import com.axiomine.largecollections.utilities.FileSerDeUtils;

public class KVCustomMapBasicTest {
    
    @Test
    public void test00BasicTest() {
        FastKVMap<Integer,Integer> map = null;
        try {
            String kser = "com.axiomine.largecollections.functions.IntegerSerDe$SerFunction";
            String vser = "com.axiomine.largecollections.functions.IntegerSerDe$SerFunction";
            String kdeser = "com.axiomine.largecollections.functions.IntegerSerDe$DeSerFunction";
            String vdeser = "com.axiomine.largecollections.functions.IntegerSerDe$DeSerFunction";
            map = new FastKVMap("c:/tmp/", "cacheMap",kser,vser,kdeser,vdeser);
            Assert.assertTrue(map.isEmpty());
            for (int i = 0; i < 10; i++) {
                int r = map.put(i, i);
                Assert.assertEquals(r, i);
            }
            Assert.assertFalse(map.isEmpty());
            Assert.assertEquals(10, map.size());
            
            int i = map.remove(0);
            Assert.assertEquals(0, i);
            
            Integer nullI = map.remove(0);
            Assert.assertNull(nullI);
            
            Assert.assertTrue(map.containsKey(1));
            Assert.assertFalse(map.containsKey(0));
            
            try {
                map.containsValue(1);
            } catch (Exception ex) {
                boolean b = ex instanceof UnsupportedOperationException;
                Assert.assertTrue(b);
            }
            Assert.assertEquals(9, map.size());
            
            
            
            Map<Integer,Integer> m = new HashMap<Integer,Integer>();
            m.put(0,0);
            m.put(1,11);
            m.put(2,22);
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
            Collection<Integer> vs = map.values();
            for(Integer s:vs){
                System.out.println(s);
            }
            System.out.println("-");
            iter = vs.iterator();
            while(iter.hasNext()){
                System.out.println(iter.next());
            }

            System.out.println("EntrySet");
            Set<Map.Entry<Integer, Integer>> es = map.entrySet();
            
            for(Map.Entry<Integer, Integer> e:es){
                System.out.println(e.getKey() +"="+e.getValue());
            }
            System.out.println("-");
            Iterator<Map.Entry<Integer, Integer>> iter2 = es.iterator();
            while(iter2.hasNext()){
                Map.Entry<Integer, Integer> e = iter2.next();
                System.out.println(e.getKey() +"="+e.getValue());
            }
            
            System.out.println("Now closing");
            map.close();
            boolean b = false;
            try{
                map.put(0,0);    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertTrue(b);
            
            System.out.println("Now Reopening");
            map.open();
            
            b = false;
            try{
                map.put(0,0);    
            }
            catch(Exception ex){
                b=true;
            }
            Assert.assertFalse(b);
            
            System.out.println("First Serialize");
            FileSerDeUtils.serializeToFile(map,new File("c:/tmp/x.ser"));
            
            map = (FastKVMap<Integer,Integer>) FileSerDeUtils.deserializeFromFile(new File("c:/tmp/x.ser"));
            map.clear();
            System.out.println(map.size());
            map.put(0,0);    
            System.out.println(map.size());
            System.out.println("Finally Destroying");
            map.destroy();
            b = false;
            try{
                map.put(0,0);    
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
